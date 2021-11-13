from rest_auth.models import User


def create_test_users():
    admin_user = User(username='admin', is_staff=True, is_active=True)
    admin_user.set_password('admin')
    admin_user.save()
    ordinary_user1 = User(username='ordinary_user1')
    ordinary_user1.set_password('ordinary_user1')
    ordinary_user1.save()
