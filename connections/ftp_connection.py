import paramiko

class Ftp_connection:
    
    def __init__(self, port, server, username, password):
        self.server = server
        self.username = username
        self.password = password
        self.ftp = None
        self.transport = None
        self.port = port
        self.error_detail = None


    def connect(self):
        try:
            self.transport = paramiko.Transport((self.server, self.port))
            self.transport.connect(username=self.username, password=self.password)

            self.ftp = paramiko.SFTPClient.from_transport(self.transport)
            return True
                
        except Exception as e:
            self.error_detail = str(e)
            return False


    def connect_with_keyfile(self):
        try:
            # Load private key from file
            key = paramiko.RSAKey.from_private_key_file(self.password)

            # Establish SFTP connection
            self.transport = paramiko.Transport((self.server, self.port))
            self.transport.connect(username=self.username, pkey=key)

            self.ftp = paramiko.SFTPClient.from_transport(self.transport)
            return True

        except Exception as e:
            self.error_detail = str(e)
            return False


    def disconnect_sftp(self):
        if self.ftp:
            self.ftp.close()
                
        if self.transport.is_active():
            self.transport.close()



    def ensure_sftp_directory_exists(self,directory):
   
        dirs = directory.split("/")
        current_dir = ""
        for dir in dirs:
            if dir:  # Skip empty parts
                current_dir += f"/{dir}"
                try:
                    self.ftp.chdir(current_dir)
                except IOError:  # Directory does not exist
                    print(f"Directory '{current_dir}' does not exist. Creating...")
                    self.ftp.mkdir(current_dir)
                    self.ftp.chdir(current_dir)
                    print(f"Created directory: {current_dir}")
           