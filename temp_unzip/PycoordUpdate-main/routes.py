import json
import logging
import os
from datetime import datetime
from functools import wraps

from flask import render_template, flash, redirect, url_for, request, abort, jsonify
from flask_login import LoginManager, login_user, logout_user, current_user, login_required
from urllib.parse import urlparse

from app import app, db
from models_sql import User, WebConfig, AuditLog, ServerConfig, DashboardStats
from forms import LoginForm, RegistrationForm, ServerConfigForm, ServerEditForm, ProfileForm

# Set up login manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


@login_manager.user_loader
def load_user(user_id):
    """Load user by ID for flask-login"""
    return User.query.get(int(user_id))


def admin_required(f):
    """Decorator for views that require admin access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('You need administrator privileges to access this page.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


def log_admin_action(action, details):
    """Log an admin action to the audit log"""
    if current_user.is_authenticated:
        log = AuditLog(
            user_id=current_user.id,
            action=action,
            details=details,
            ip_address=request.remote_addr
        )
        db.session.add(log)
        db.session.commit()


# Routes
@app.route('/')
def index():
    """Home page"""
    last_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return render_template('index.html', title='Home', last_started=last_started)


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password', 'danger')
            return redirect(url_for('login'))
        
        login_user(user, remember=form.remember_me.data)
        user.last_login = datetime.utcnow()
        db.session.commit()
        
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        
        return redirect(next_page)
    
    return render_template('login.html', title='Sign In', form=form)


@app.route('/logout')
def logout():
    """Logout route"""
    logout_user()
    return redirect(url_for('index'))


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Registration page"""
    # Check if registration is allowed
    allow_registration = WebConfig.query.filter_by(key='allow_registration').first()
    if allow_registration and allow_registration.value.lower() != 'true':
        flash('Registration is currently disabled.', 'warning')
        return redirect(url_for('index'))
    
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(
            username=form.username.data,
            email=form.email.data
        )
        user.set_password(form.password.data)
        
        # First user is automatically an admin
        if User.query.count() == 0:
            user.is_admin = True
        
        db.session.add(user)
        db.session.commit()
        
        flash('Congratulations, you are now registered!', 'success')
        return redirect(url_for('login'))
    
    return render_template('register.html', title='Register', form=form)


@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """User profile page"""
    form = ProfileForm(original_username=current_user.username, original_email=current_user.email)
    
    if form.validate_on_submit():
        if form.current_password.data and not current_user.check_password(form.current_password.data):
            flash('Current password is incorrect', 'danger')
            return redirect(url_for('profile'))
        
        current_user.username = form.username.data
        current_user.email = form.email.data
        current_user.discord_id = form.discord_id.data
        
        if form.new_password.data:
            current_user.set_password(form.new_password.data)
        
        db.session.commit()
        flash('Your profile has been updated.', 'success')
        return redirect(url_for('profile'))
    
    elif request.method == 'GET':
        form.username.data = current_user.username
        form.email.data = current_user.email
        form.discord_id.data = current_user.discord_id
    
    return render_template('profile.html', title='Profile', form=form)


@app.route('/dashboard')
@login_required
def dashboard():
    """User dashboard"""
    return render_template('dashboard.html', title='Dashboard')


@app.route('/servers')
@login_required
def servers():
    """List of servers"""
    server_configs = ServerConfig.query.all()
    return render_template('servers.html', title='Servers', servers=server_configs)


@app.route('/servers/add', methods=['GET', 'POST'])
@login_required
def add_server():
    """Add a new server configuration"""
    form = ServerConfigForm()
    
    if form.validate_on_submit():
        # Check if server already exists
        existing = ServerConfig.query.filter_by(
            discord_guild_id=request.form.get('discord_guild_id'),
            server_id=form.server_id.data
        ).first()
        
        if existing:
            flash('A server with this ID already exists for this Discord guild.', 'danger')
            return redirect(url_for('add_server'))
        
        server = ServerConfig(
            discord_guild_id=request.form.get('discord_guild_id'),
            server_id=form.server_id.data,
            server_name=form.server_name.data,
            sftp_host=form.sftp_host.data,
            sftp_username=form.sftp_username.data,
            sftp_password=form.sftp_password.data,
            sftp_port=form.sftp_port.data,
            log_path=form.log_path.data,
            premium_tier=form.premium_tier.data,
            features_enabled=json.dumps({
                "killfeed": True,
                "economy": form.premium_tier.data >= 2,
                "rivalries": form.premium_tier.data >= 3,
                "factions": form.premium_tier.data >= 2,
                "bounties": form.premium_tier.data >= 2,
                "events": form.premium_tier.data >= 1
            })
        )
        
        db.session.add(server)
        db.session.commit()
        
        if current_user.is_admin:
            log_admin_action('server_add', f'Added server configuration for {form.server_name.data}')
        
        flash('Server configuration added successfully.', 'success')
        return redirect(url_for('servers'))
    
    return render_template('server_form.html', title='Add Server', form=form)


@app.route('/servers/edit/<int:server_id>', methods=['GET', 'POST'])
@login_required
def edit_server(server_id):
    """Edit a server configuration"""
    server = ServerConfig.query.get_or_404(server_id)
    form = ServerEditForm()
    
    if form.validate_on_submit():
        server.server_name = form.server_name.data
        server.sftp_host = form.sftp_host.data
        server.sftp_username = form.sftp_username.data
        if form.sftp_password.data:
            server.sftp_password = form.sftp_password.data
        server.sftp_port = form.sftp_port.data
        server.log_path = form.log_path.data
        server.premium_tier = form.premium_tier.data
        
        if form.features_enabled.data:
            try:
                features = json.loads(form.features_enabled.data)
                server.features_enabled = json.dumps(features)
            except json.JSONDecodeError:
                flash('Invalid JSON format for features', 'danger')
                return redirect(url_for('edit_server', server_id=server_id))
        
        db.session.commit()
        
        if current_user.is_admin:
            log_admin_action('server_edit', f'Edited server configuration for {server.server_name}')
        
        flash('Server configuration updated successfully.', 'success')
        return redirect(url_for('servers'))
    
    elif request.method == 'GET':
        form.server_name.data = server.server_name
        form.sftp_host.data = server.sftp_host
        form.sftp_username.data = server.sftp_username
        form.sftp_port.data = server.sftp_port
        form.log_path.data = server.log_path
        form.premium_tier.data = server.premium_tier
        form.features_enabled.data = server.features_enabled
    
    return render_template('server_form.html', title='Edit Server', form=form, server=server)


@app.route('/servers/delete/<int:server_id>', methods=['POST'])
@login_required
@admin_required
def delete_server(server_id):
    """Delete a server configuration"""
    server = ServerConfig.query.get_or_404(server_id)
    
    server_name = server.server_name
    db.session.delete(server)
    db.session.commit()
    
    log_admin_action('server_delete', f'Deleted server configuration for {server_name}')
    
    flash('Server configuration deleted successfully.', 'success')
    return redirect(url_for('servers'))


@app.route('/admin')
@login_required
@admin_required
def admin():
    """Admin dashboard"""
    users = User.query.all()
    servers = ServerConfig.query.all()
    audit_logs = AuditLog.query.order_by(AuditLog.timestamp.desc()).limit(50).all()
    
    return render_template('admin.html', title='Admin Dashboard', 
                          users=users, servers=servers, audit_logs=audit_logs)


@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    """Admin user management"""
    users = User.query.all()
    return render_template('admin_users.html', title='User Management', users=users)


@app.route('/admin/users/toggle-admin/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def toggle_admin(user_id):
    """Toggle admin status for a user"""
    user = User.query.get_or_404(user_id)
    
    # Don't allow removing admin from yourself
    if user.id == current_user.id:
        flash('You cannot remove your own admin privileges.', 'danger')
        return redirect(url_for('admin_users'))
    
    user.is_admin = not user.is_admin
    db.session.commit()
    
    action = 'Granted' if user.is_admin else 'Removed'
    log_admin_action('toggle_admin', f'{action} admin privileges for user {user.username}')
    
    flash(f'Admin status for {user.username} has been updated.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/delete/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user"""
    user = User.query.get_or_404(user_id)
    
    # Don't allow deleting yourself
    if user.id == current_user.id:
        flash('You cannot delete your own account.', 'danger')
        return redirect(url_for('admin_users'))
    
    username = user.username
    db.session.delete(user)
    db.session.commit()
    
    log_admin_action('delete_user', f'Deleted user {username}')
    
    flash(f'User {username} has been deleted.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/config', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_config():
    """Admin configuration page"""
    if request.method == 'POST':
        for key, value in request.form.items():
            if key.startswith('config_'):
                config_key = key[7:]  # Remove 'config_' prefix
                config = WebConfig.query.filter_by(key=config_key).first()
                
                if config:
                    config.value = value
                else:
                    config = WebConfig(key=config_key, value=value)
                    db.session.add(config)
        
        db.session.commit()
        log_admin_action('update_config', 'Updated web configuration settings')
        flash('Configuration updated successfully.', 'success')
        return redirect(url_for('admin_config'))
    
    configs = WebConfig.query.all()
    config_dict = {c.key: c.value for c in configs}
    
    # Ensure basic configs exist
    default_configs = {
        'site_name': 'Tower of Temptation PvP Stats',
        'allow_registration': 'true',
        'discord_bot_invite': '',
        'support_email': '',
        'discord_server_invite': ''
    }
    
    for key, value in default_configs.items():
        if key not in config_dict:
            config = WebConfig(key=key, value=value)
            db.session.add(config)
            config_dict[key] = value
    
    db.session.commit()
    
    return render_template('admin_config.html', title='Site Configuration',
                          configs=config_dict)


@app.route('/api/stats/<discord_guild_id>/<server_id>/<stat_type>')
def api_stats(discord_guild_id, server_id, stat_type):
    """API endpoint for getting cached stats"""
    stat = DashboardStats.query.filter_by(
        discord_guild_id=discord_guild_id,
        server_id=server_id,
        stat_type=stat_type
    ).first()
    
    if not stat:
        return jsonify({'error': 'Stats not found'}), 404
    
    try:
        data = json.loads(stat.stat_data)
        return jsonify(data)
    except json.JSONDecodeError:
        return jsonify({'error': 'Invalid data format'}), 500


@app.route('/api/update-stats', methods=['POST'])
def api_update_stats():
    """API endpoint for updating cached stats"""
    # This would need authentication in production
    try:
        data = request.json
        discord_guild_id = data.get('discord_guild_id')
        server_id = data.get('server_id')
        stat_type = data.get('stat_type')
        stat_data = data.get('stat_data')
        
        if not discord_guild_id or not server_id or not stat_type or not stat_data:
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Verify the server exists
        server = ServerConfig.query.filter_by(
            discord_guild_id=discord_guild_id,
            server_id=server_id
        ).first()
        
        if not server:
            return jsonify({'error': 'Server not found'}), 404
        
        # Update or create the stat
        stat = DashboardStats.query.filter_by(
            discord_guild_id=discord_guild_id,
            server_id=server_id,
            stat_type=stat_type
        ).first()
        
        if stat:
            stat.stat_data = json.dumps(stat_data)
            stat.updated_at = datetime.utcnow()
        else:
            stat = DashboardStats(
                discord_guild_id=discord_guild_id,
                server_id=server_id,
                stat_type=stat_type,
                stat_data=json.dumps(stat_data)
            )
            db.session.add(stat)
        
        db.session.commit()
        return jsonify({'success': True})
    
    except Exception as e:
        logging.error(f"Error updating stats: {str(e)}")
        return jsonify({'error': str(e)}), 500


# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('errors/404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('errors/500.html'), 500


# Template context processors
@app.context_processor
def inject_site_config():
    """Inject site configuration into all templates"""
    try:
        configs = WebConfig.query.all()
        config_dict = {c.key: c.value for c in configs}
    except Exception:
        # If table doesn't exist yet, return empty dict
        config_dict = {
            'site_name': 'Tower of Temptation PvP Stats',
            'allow_registration': 'true'
        }
    
    return {'site_config': config_dict}