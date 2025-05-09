"""
Forms for the Tower of Temptation PvP Statistics Bot web interface
"""
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, IntegerField, TextAreaField
from wtforms.validators import DataRequired, Email, EqualTo, ValidationError, Length, Optional, NumberRange
from models_sql import User

class LoginForm(FlaskForm):
    """Login form"""
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')


class RegistrationForm(FlaskForm):
    """Registration form"""
    username = StringField('Username', validators=[DataRequired(), Length(min=3, max=64)])
    email = StringField('Email', validators=[DataRequired(), Email(), Length(max=120)])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=8)])
    password2 = PasswordField('Repeat Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Register')

    def validate_username(self, username):
        """Validate username"""
        user = User.query.filter_by(username=username.data).first()
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        """Validate email"""
        user = User.query.filter_by(email=email.data).first()
        if user is not None:
            raise ValidationError('Please use a different email address.')


class ProfileForm(FlaskForm):
    """User profile form"""
    username = StringField('Username', validators=[DataRequired(), Length(min=3, max=64)])
    email = StringField('Email', validators=[DataRequired(), Email(), Length(max=120)])
    discord_id = StringField('Discord User ID', validators=[Optional(), Length(max=64)])
    current_password = PasswordField('Current Password', validators=[Optional()])
    new_password = PasswordField('New Password', validators=[Optional(), Length(min=8)])
    confirm_password = PasswordField('Confirm New Password', validators=[EqualTo('new_password')])
    submit = SubmitField('Update Profile')

    def __init__(self, original_username=None, original_email=None, *args, **kwargs):
        super(ProfileForm, self).__init__(*args, **kwargs)
        self.original_username = original_username
        self.original_email = original_email

    def validate_username(self, username):
        """Validate username"""
        if username.data != self.original_username:
            user = User.query.filter_by(username=username.data).first()
            if user is not None:
                raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        """Validate email"""
        if email.data != self.original_email:
            user = User.query.filter_by(email=email.data).first()
            if user is not None:
                raise ValidationError('Please use a different email address.')


class ServerConfigForm(FlaskForm):
    """Server configuration form"""
    discord_guild_id = StringField('Discord Guild ID', validators=[DataRequired(), Length(max=64)])
    server_id = StringField('Server ID', validators=[DataRequired(), Length(max=64)])
    server_name = StringField('Server Name', validators=[DataRequired(), Length(max=128)])
    sftp_host = StringField('SFTP Host', validators=[DataRequired(), Length(max=128)])
    sftp_username = StringField('SFTP Username', validators=[DataRequired(), Length(max=64)])
    sftp_password = PasswordField('SFTP Password', validators=[DataRequired(), Length(max=128)])
    sftp_port = IntegerField('SFTP Port', validators=[Optional(), NumberRange(min=1, max=65535)], default=22)
    log_path = StringField('Log File Path', validators=[DataRequired(), Length(max=256)])
    premium_tier = IntegerField('Premium Tier', validators=[NumberRange(min=0, max=5)], default=0)
    submit = SubmitField('Add Server')


class ServerEditForm(FlaskForm):
    """Server edit form"""
    server_name = StringField('Server Name', validators=[DataRequired(), Length(max=128)])
    sftp_host = StringField('SFTP Host', validators=[DataRequired(), Length(max=128)])
    sftp_username = StringField('SFTP Username', validators=[DataRequired(), Length(max=64)])
    sftp_password = PasswordField('SFTP Password', validators=[Optional(), Length(max=128)])
    sftp_port = IntegerField('SFTP Port', validators=[Optional(), NumberRange(min=1, max=65535)], default=22)
    log_path = StringField('Log File Path', validators=[DataRequired(), Length(max=256)])
    premium_tier = IntegerField('Premium Tier', validators=[NumberRange(min=0, max=5)], default=0)
    features_enabled = TextAreaField('Features Enabled (JSON)', validators=[Optional()])
    submit = SubmitField('Update Server')