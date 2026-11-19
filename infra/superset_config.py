import os
SECRET_KEY = os.environ['SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'
WTF_CSRF_ENABLED = True
FEATURE_FLAGS = {'ENABLE_TEMPLATE_PROCESSING': False}
TALISMAN_ENABLED = False  # loopback-only educational HTTP deployment
