


class ApiConnekta():
    url_base="https://serviciosqa.siesacloud.com"

    conni_key="b5e8d5c1ee5e1c3c2d3519ef018d2709"
    conni_token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjA1YTBkYTlhLWJjMDUtNDAyNS1iOTU4LTQ5ZjU2Yzk5MjA4MiIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwOC8wNi9pZGVudGl0eS9jbGFpbXMvcHJpbWFyeXNpZCI6IjAxYzcyYTFjLTE5ODItNDE3MC1hZmExLTM5OGFlYjBjZTUzOCJ9.ie4BS4dZ0qDXvhEAO2b8X4N93HbQ6j0gPpx3gaJ1GzE"

    id_compania="8703"
    id_sistema="6"
    url_parametros_sistema=f"/api/Connekta/v3/parametrosporsistema?idCompania={id_compania}&idSistema={id_sistema}"

    # url_base="https://servicios.siesacloud.com"

    # conni_key="a90cebe32606a7f1dd50ea410228a0f6"
    # conni_token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjdjNjY3MWRkLTYyNTUtNDBmNy04MzQ2LThhMzNmOGM3ZTJhYSIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwOC8wNi9pZGVudGl0eS9jbGFpbXMvcHJpbWFyeXNpZCI6IjcyMjljZDQ1LTQ0MTAtNDBiOS1hMTIwLTlkMTgyMWUxNGEyNCJ9.ze84dsU3bFq7zytynrAeuDtwDRC9kgGL40TyG5IBfjw"

    # id_compania="8703"
    # id_sistema="6"
    # url_parametros_sistema=f"/api/Connekta/v3/parametrosporsistema?idCompania={id_compania}&idSistema={id_sistema}"
    id_interfaces_base_correo = 176

    servicio_log ="/api/connekta/v3/gestionarlog?"


class properties():
    id_corte=None
    ciclo=None
    port_ftp_paradigma_email= None
    server_ftp_paradigma_email= None
    user_ftp_paradigma_email= None
    password_ftp_paradigma_email= None
    path_base_ftp_paradigma_email = None
    path_base_local = None

properties=properties()

