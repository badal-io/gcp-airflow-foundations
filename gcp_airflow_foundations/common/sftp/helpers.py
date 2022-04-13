from airflow.models import Variable


def save_id_to_file(private_key_var):
    key = Variable.get(private_key_var)
    secret_path = "id_rsa"
    with open(secret_path, "w") as f:
        f.write(key)
