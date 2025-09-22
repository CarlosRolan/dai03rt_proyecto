from db_conn import reset_tmdb, create_role_if_needed, grant_tmdb_privs

def lambda_handler(event, context):
    
    # Si no hay parametros se ejecutan los ficheros
    fb_reset = reset_tmdb()
    fb_user_creation = create_role_if_needed()
    fb_privileges = grant_tmdb_privs()

    feedback = {
        'reset_db': fb_reset,
        'user_creation': fb_user_creation,
        'privileges': fb_privileges
    }

    return {
        'statusCode': 200,
        'body': feedback
    }