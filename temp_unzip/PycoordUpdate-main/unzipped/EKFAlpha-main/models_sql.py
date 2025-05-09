"""
SQL Models for the Tower of Temptation PvP Statistics Bot web interface
"""
from datetime import datetime
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

from app import db


class User(UserMixin, db.Model):
    """User model for authentication and admin access"""
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256))
    is_admin = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)
    discord_id = db.Column(db.String(64), unique=True)
    
    def set_password(self, password):
        """Set user password"""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Check user password"""
        return check_password_hash(self.password_hash, password)


class WebConfig(db.Model):
    """Configuration for web interface"""
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    value = db.Column(db.Text)
    description = db.Column(db.Text)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AuditLog(db.Model):
    """Audit log for admin actions"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    action = db.Column(db.String(64), nullable=False)
    details = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    ip_address = db.Column(db.String(64))
    
    user = db.relationship('User', backref=db.backref('audit_logs', lazy=True))


class ServerConfig(db.Model):
    """Server configuration from the web interface"""
    id = db.Column(db.Integer, primary_key=True)
    discord_guild_id = db.Column(db.String(64), nullable=False)
    server_id = db.Column(db.String(64), nullable=False)
    server_name = db.Column(db.String(128))
    sftp_host = db.Column(db.String(128))
    sftp_username = db.Column(db.String(64))
    sftp_password = db.Column(db.String(128))
    sftp_port = db.Column(db.Integer, default=22)
    log_path = db.Column(db.String(256))
    premium_tier = db.Column(db.Integer, default=0)
    features_enabled = db.Column(db.Text)  # JSON string of enabled features
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('discord_guild_id', 'server_id', name='unique_server_config'),
    )


class DashboardStats(db.Model):
    """Cached statistics for the dashboard"""
    id = db.Column(db.Integer, primary_key=True)
    discord_guild_id = db.Column(db.String(64), nullable=False)
    server_id = db.Column(db.String(64), nullable=False)
    stat_type = db.Column(db.String(64), nullable=False)  # kills, deaths, rivalries, etc.
    stat_data = db.Column(db.Text)  # JSON string of stat data
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('discord_guild_id', 'server_id', 'stat_type', name='unique_stat'),
    )