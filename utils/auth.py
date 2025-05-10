# utils/auth.py

# Dummy user data for demonstration
# In a real application, this would come from a database or secure storage
USERS = {
    "admin": "password123",
    "user1": "test"
}

def authenticate_user(username, password):
    """Checks if the provided username and password are valid."""
    if username in USERS and USERS[username] == password:
        return True
    return False

def get_user(username):
    """Retrieves user information (can be expanded later)."""
    if username in USERS:
        return {"username": username}
    return None