import os
from connections import ftp_connection
from log_book import logger_config
from config.config import properties

def download_file_ftp_siesa(remote_file_path, local_file_path):
    try:
        connection = ftp_connection.Ftp_connection(int(properties.port_ftp_siesa), properties.server_ftp_siesa, properties.user_ftp_siesa, properties.path_key_file_ftp_siesa)
        request = connection.connect_with_keyfile()

        if request:
            # Ensure the local directory exists
            validate_or_create_directory_for_file(local_file_path)

            if os.path.exists(local_file_path):
               
                print(f"The file {os.path.basename(local_file_path)} already exists. It will not be downloaded again.")
                return False
            
            try:
                connection.ftp.get(remote_file_path, local_file_path)
                print(f"File {os.path.basename(local_file_path)} downloaded successfully.")
                return True
            except FileNotFoundError:
                print(f"Remote file not found: {remote_file_path}")
                return False
            except Exception as e:
                print(f"Failed to download file {os.path.basename(local_file_path)}: {str(e)}")
                return False
        else:
            print(f"Connection error: {connection.error_detail}")
            return False

        connection.disconnect_sftp()
    except Exception as e:
        print(f"An error occurred during the FTP server connection: {str(e)}")
        return False


def download_file_ftp_paradigma(remote_file_path, local_file_path):
    try:
        #connection = ftp_connection.Ftp_connection(int(properties.port_ftp_siesa), properties.server_ftp_siesa, properties.user_ftp_siesa, properties.path_key_file_ftp_siesa)
        #request = connection.connect_with_keyfile()
        connection = ftp_connection.Ftp_connection(int(properties.port_ftp_paradigma_email), properties.server_ftp_paradigma_email, properties.user_ftp_paradigma_email, properties.password_ftp_paradigma_email)
        request = connection.connect()
        if request:
            # Ensure the local directory exists
            validate_or_create_directory_for_file(local_file_path)

            if os.path.exists(local_file_path):
                print(f"The file {os.path.basename(local_file_path)} already exists. It will not be downloaded again.")
                return False
            
            try:
                connection.ftp.get(remote_file_path, local_file_path)
                print(f"File {os.path.basename(local_file_path)} downloaded successfully.")
                return True
            except FileNotFoundError:
                print(f"Remote file not found: {remote_file_path}")
                return False
            except Exception as e:
                print(f"Failed to download file {os.path.basename(local_file_path)}: {str(e)}")
                return False
        else:
            print(f"Connection error: {connection.error_detail}")
            return False

        connection.disconnect_sftp()
    except Exception as e:
        print(f"An error occurred during the FTP server connection: {str(e)}")
        return False

def validate_or_create_directory_for_file(file_path):
   
    directory_path = os.path.dirname(file_path)
    
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            print(f"Directory created: {directory_path}")
        except OSError as e:
            print(f"Error creating directory {directory_path}: {str(e)}")
    else:
        print(f"Directory already exists: {directory_path}")
