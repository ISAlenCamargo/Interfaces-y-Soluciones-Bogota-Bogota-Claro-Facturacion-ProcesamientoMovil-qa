from process_modules.data_formatting.data_formatting  import *
import re
from process_modules.data_formatting.data_formatting import limpiezaDireccion
def generarFe(factura,bach_id_lote,email,lista_dane,corte,fecha_limite_pago):
 try:
    expresion_regu_caract_especiales=r'[^a-zA-Z0-9\s]'
    totalIntereses=0
    rowsArrays1160=[]
    # rows_array_1330=[]
    # rows_array_1430=[]
    array_ITM=[]
    array_ITX=[]
    array_TAX=[]

    indicador_linea_1330=False

    count_1330_repo_finan=0
    count_1330_repo_finan_impuesto=0

    # count_1430_repo_finan=0
    # count_1430_repo_finan_impuesto=0
    datos_ITM = {
        "tipo_de_registro":                            "ITM",
        "consecutivo":                                 None,
        "cantidad":                                    "1.00",
        "unidad_de_cantidad":                          "94",
        "valor_unidad":                                None,
        "valor_total":                                 None,
        "descripcion":                                 None,
        "nombre_canal":                                "",
        "codigo_item":                                 None,
        "valor_recargo_descuento":                     "0.00",
        "valor_total_dolares":                         ""
    }
    datos_ITX={
        "tipo_de_registro":                          "ITX",
        "consecutivo":                               None,
        "consecutivo_item":                          None,
        "id_impuesto":                               None,
        "retenido":                                  "false",
        "porcentaje":                                None,
        "base":                                      None,
        "valor_impuesto":                            None

    }
    datos_TAX={
        "tipo_de_registro":                     "TAX",
        "id_impuesto":                          None,
        "retenido":                             "false",
        "porcentaje":                           None,
        "base":                                 "0",
        "valor_impuesto":                       "0",
        "nombre_canales":                       "",
        "codigo_items":                         ""
    }
    registro_ITX=0
    existe_tag_1115=False
    for linea in factura:
        
        if linea.startswith("10 "):
                codigo_dane=linea[79:114].strip()
                codigo_dane=codigo_dane[2:7]


        if linea.startswith("11 "):
            invoiceDate=linea[521:529].strip()
            invoiceNumber=linea[368:378].strip()
            cust_code=linea[378:413].strip()
            tipo_persona=linea[539]
            if len(tipo_persona.strip())==0:tipo_persona="1"
            municipio_departamento=linea[323:363].strip()
            validacion_puntoComa=linea.find(";",541)
            documento_persona=linea[541:validacion_puntoComa].strip()

            nombre_pesona=linea[8:78].strip()
            nombre_pesona=nombre_pesona[nombre_pesona.find(".")+1:].strip()
            nombre_pesona=limpiezaNombre(nombre_pesona)

            ciudad_departamento=linea[323:363].strip()
            direccion=linea[148:218].strip()
            
            telefono=linea[413:433].strip()


        if linea.startswith("1110 "):
                                
                subtotal=float(linea[80:98].strip())
                total_impuestos=linea[98:116].strip()
                deuda_anterior=linea[44:62].strip()
                total_a_pagar=linea[116:134].strip()
                


        if linea.startswith("1115 "):
            
            total_descuento=float(linea[116:134].strip())

            total_subscription=linea[8:26].strip()
            tax_total_subscription=linea[26:44].strip()

            total_acces=linea[44:62].strip()
            tax_total_acces=linea[62:80].strip()
            total_occs=linea[80:98].strip()
            impuesto_total_occs=linea[98:116].strip()


            total_usage=linea[152:170].strip()
            total__smsu_usage=linea[188:206].strip()
            tax_total_usage=linea[170:188].strip()

            #1115_totalEquipo
            total_equipos_a_credito=linea[224:242].strip()
            total_impuesto_equipos_credito=linea[242:260].strip()

            # valor_total_servicios_adicionales=linea[44:62].strip()
            # valor_impuesto_iva_servicios_adicionales=linea[62:80].strip()

            # valor_total_otros_servicios_creditos=linea[80:98].strip()
            # valor_impuesto_otros_servicios_creditos=linea[98:116].strip()

            # existe_tag_1115=True
            

        if linea.startswith("1120 "):
                if registro_ITX==0:
                    cadena_ITX=linea
                                
                registro_ITX+=1


        # if linea.startswith("1130 "):
        
        #     fecha_limite_pago=linea[75:83].strip()


        if linea.startswith("1160 "):

                array1160=linea[4:].strip().split("|")
                rowsArrays1160.append(array1160)
                
                if len(array1160)<=10:
                    totalIntereses+=float(array1160[4])
                                    
                elif (len(array1160)>10):
                    totalIntereses+=float(array1160[6])+float(array1160[7])  #TODO: POR EL MOMENTO LO VAMOS A ENVIAR EN CERO
    
    
                           
        if  linea.startswith("1330  "):
             if any(word in linea.strip().lower() for word in ["reposicion", "financiacion", "reposición", "financiación","solicitud del suscriptor","solicitud suscriptor venta tecnologia"]):
                    indicador_linea_1330=True
                    count_1330_repo_finan+=float(linea[158:176])
                    count_1330_repo_finan_impuesto+=float(linea[176:194])
               
                  

        if linea.startswith("1430 ") and indicador_linea_1330==False:
             if any(word in linea.strip().lower() for word in ["reposicion", "financiacion", "reposición", "financiación","solicitud del suscriptor","solicitud suscriptor venta tecnologia"]):
             
                   count_1330_repo_finan+=float(linea[158:176])
                   count_1330_repo_finan_impuesto+=float(linea[176:194])
        


        
    [año,mes,dia]=splitFecha(invoiceDate)
    interes_corriente=0
    interes_mora=0
    gestion_recarga=0
    valor_impuesto_coriente_mora=0
    valor_impuesto_gestion_recarga=0
        

    for row in rowsArrays1160:
            bin=float(row[0][4:6])
            if bin!=55 and bin!=56:
                    
                    interes_corriente=interes_corriente+float(row[6])
                    interes_mora=interes_mora+float(row[7])
                    valor_impuesto_coriente_mora=valor_impuesto_coriente_mora+float(row[8])
                    #subtotal-=float(row[5])

            else:
                    if len(row)>=15:
                            gestion_recarga=float(gestion_recarga)+float(row[13])  
                            valor_impuesto_gestion_recarga= valor_impuesto_gestion_recarga+float(row[8])+float(row[15])


    ##Validaciones ITM

    if (float(total_subscription)>0):
        cargos_fijos_ITM=datos_ITM.copy()
        cargos_fijos_ITM["consecutivo"]=str(len(array_ITM)+1)
        cargos_fijos_ITM["valor_unidad"]=formatear_2Decimales(float(total_subscription))
        cargos_fijos_ITM["valor_total"]=formatear_2Decimales(float(total_subscription))
        cargos_fijos_ITM["descripcion"]="Cargos Fijos"
        cargos_fijos_ITM["codigo_item"]="CFM_M"
        cargos_fijos_ITM["nombre_canal"]="CFM_M"
        
        array_ITM.append(cargos_fijos_ITM)
    elif float(total_subscription)<0:
         total_descuento+=float(total_subscription) #TODO: ACTIVO SOLO POR PRUEBAS
   


    if float(total_acces)-float(total__smsu_usage)>0:
        servicios_adicionales_ITM=datos_ITM.copy()
        servicios_adicionales_ITM["consecutivo"]=str(len(array_ITM)+1)
        servicios_adicionales_ITM["valor_unidad"]=formatear_2Decimales(float(total_acces)-float(total__smsu_usage))
        servicios_adicionales_ITM["valor_total"]=formatear_2Decimales(float(total_acces)-float(total__smsu_usage))
        servicios_adicionales_ITM["descripcion"]="Servicios adicionales"
        servicios_adicionales_ITM["codigo_item"]="OSA_M"
        servicios_adicionales_ITM["nombre_canal"]="OSA_M"

        array_ITM.append(servicios_adicionales_ITM)
    elif float(total_acces)<0: total_descuento+=float(total_acces)


    if float(total_occs) -abs(count_1330_repo_finan)>0:
        otros_servicios_ITM=datos_ITM.copy()
        otros_servicios_ITM["consecutivo"]=str(len(array_ITM)+1)
        otros_servicios_ITM["valor_unidad"]=formatear_2Decimales(float(total_occs)-count_1330_repo_finan)
        otros_servicios_ITM["valor_total"]=formatear_2Decimales(float(total_occs)-count_1330_repo_finan)
        otros_servicios_ITM["descripcion"]="Otros servicios OCCs"
        otros_servicios_ITM["codigo_item"]="OCC_M"
        otros_servicios_ITM["nombre_canal"]="OCC_M"

        array_ITM.append(otros_servicios_ITM)
    elif float(total_occs)-abs(count_1330_repo_finan)<0: total_descuento+=float(total_occs)+(-1*abs(count_1330_repo_finan))




    #------------------------------------------------------------------------------------
    if (float(total_usage)+float(total__smsu_usage))>0:
        otros_consumo_itm=datos_ITM.copy()
        otros_consumo_itm["consecutivo"]=str(len(array_ITM)+1)
        otros_consumo_itm["valor_unidad"]=formatear_2Decimales(float(total__smsu_usage)+float(total_usage))
        otros_consumo_itm["valor_total"]=formatear_2Decimales(float(total__smsu_usage)+float(total_usage))
        otros_consumo_itm["descripcion"]="Otros consumos"
        otros_consumo_itm["codigo_item"]="CONS_M"
        otros_consumo_itm["nombre_canal"]="CONS_M"
        

        array_ITM.append(otros_consumo_itm)
    elif float(total__smsu_usage)+float(total_usage)<0: total_descuento+=(float(total__smsu_usage)+float(total_usage))


    if interes_corriente>0 or interes_mora>0:
        inter_eq_financiados=datos_ITM.copy()
        inter_eq_financiados["consecutivo"]=str(len(array_ITM)+1)
        inter_eq_financiados["valor_unidad"]=formatear_2Decimales(interes_corriente+interes_mora)
        inter_eq_financiados["valor_total"]=formatear_2Decimales(interes_corriente+interes_mora)
        inter_eq_financiados["descripcion"]="Interés Equipos financiados"
        inter_eq_financiados["codigo_item"]="IEF_M"
        inter_eq_financiados["nombre_canal"]="IEF_M"
        array_ITM.append(inter_eq_financiados)
        
    elif interes_corriente+interes_mora<0: total_descuento+=interes_corriente+interes_mora


    if gestion_recarga>0:
        gestion_prestamo_ITM=datos_ITM.copy()
        gestion_prestamo_ITM["consecutivo"]=str(len(array_ITM)+1)
        gestion_prestamo_ITM["valor_unidad"]=formatear_2Decimales(gestion_recarga)
        gestion_prestamo_ITM["valor_total"]=formatear_2Decimales(gestion_recarga)
        gestion_prestamo_ITM["descripcion"]="Gestion prestamo tu desvare"
        gestion_prestamo_ITM["codigo_item"]="GPD_M"
        gestion_prestamo_ITM["nombre_canal"]="GPD_M"
        array_ITM.append(gestion_prestamo_ITM)
    elif gestion_recarga<0: total_descuento+=gestion_recarga

    [iva,consumo]=formatearIvaImpuestosITX(cadena_ITX)
    TAX_iva=datos_TAX.copy()
    TAX_iva["id_impuesto"]="01"
    TAX_iva["porcentaje"]="0.19"
    #iva=float(iva)+float(valor_impuesto_coriente_mora)+float(valor_impuesto_gestion_recarga) #TODO: MODIFICACION TEMPORAL 
    


    datos_PRC = {
        "tipo_de_registro":                "PRC",
        "proveedor_tecnologico":           "SIE",
        "codigo_facturador":               "CMC",
        "nit_facturador":                  "800153993",
        "producto":                        "SM", 
        "año":                             año,
        "mes":                             mes,
        "corte":                           str(corte), 
        "lote":                            str(bach_id_lote),  
        "version_facturacion_electronica": "2.1"
    }


    datos_CAB={
        "tipo_de_registro":                       "CAB",
        "tipo_de_documento":                      "FC",
        "tipo_de_factura":                        "1",
        "identificador_de_moneda":                "COP",
        "prefijo_factura":                        "E", #TODO:propiedades_Connekta.prefijo_factura,
        "numero_factura":                         str(invoiceNumber),
        "CUFE":                                   "",
        "fecha_factura":                          formatearFechaGuiones(get_current_date()),
        "hora_factura":                           getHoraFormatoPuntos(),
        "total_intereses":                        "0.00",   #TODO: str(totalIntereses)
        "total_descuento":                        formatear_2Decimales(abs(total_descuento)),
        "subtotal":                               formatear_2Decimales(subtotal),
        "total_impuestos":                        formatear_2Decimales(float(total_impuestos)),
        "total_del_mes":                          formatear_2Decimales(round(float(subtotal)+float(total_impuestos),2)),
        "deuda_anterior":                         deuda_anterior,
        "total_a_pagar":                          total_a_pagar,
        "nota":                                   f"Factura móvil - {"E"} {invoiceNumber}-{cust_code}",
        "total_retefuente":                       "0.00",
        "prefijo_factura_afectada":               "",
        "numero_factura_afectada":                "",
        "forma_de_pago":                          "1",
        "medio_de_pago":                          "1",
        "fecha_vencimiento_factura":              formatearFechaGuiones(fecha_limite_pago),
        "TRM":                                    "",
        "fecha_TRM":                              "",
        "ajuste_al_peso":                         "0",
        "numero_orden_compra":                    "",
        "fecha_orden_compra":                     "",
        "codigo_correccion":                      "",
        "period_factura_afectada":                ""

    }
    
    codigo_dane2= generar_codigo_dane(municipio_departamento,lista_dane)

    if len(codigo_dane2)>0:
        codigo_dane=codigo_dane2

    if not limpiar_identificacion(documento_persona) or not nombre_pesona or len(documento_persona)<4:
          documento_persona="222222222222"
          nombre_pesona="Consumidor Final"
    else:
         if type_document_logic[tipo_persona]!="41":
            documento_persona=limpiar_identificacion(documento_persona)
    


    if not contain_letters(documento_persona) or type_document_logic[tipo_persona]=="41" :
         if tipo_persona=="2":  
            documento_persona=f"{documento_persona}-{str(calculate_digito_verificacion(documento_persona))}"
    else:
         documento_persona="222222222222"
         nombre_pesona="Consumidor Final"

    
    datos_ADQ={
        "tipo_de_registro":               "ADQ",
        "tipo_de_persona":                type_person_logic[tipo_persona],
        "tipo_de_documento":              type_document_logic[tipo_persona],
        "numero_documento":               documento_persona,
        "nombres":                        limpiezaNombre(nombre_pesona)  ,
        "apellidos":                      limpiezaNombre(nombre_pesona),
        "id_pais":                        "CO",
        "codigo_dane":                    codigo_dane,
        "No usado- Campo disponible1":    ciudad_departamento,
        "No usado- Campo disponible2":    "",
        "direccion":                      limpiezaDireccion(direccion),
        "codigo_postal":                  codigo_dane,
        "telefono":                       re.sub(expresion_regu_caract_especiales,'',telefono) ,
        "correo_electronico":             email.replace(chr(31)," "),
        "tipo_regimen":                   "48",
        "actividades_economicas":         "R-99-PN",
        "responsabilidad_tributaria":     ""
    }
    
    valor_total=0 
    tax_valor_total=0
    valor_impuesto_total=0
    lineas_ITM=""
    existe_1160=False

    if float(iva)>0: 
        ITX_iva=datos_ITX.copy()
        ITX_iva["consecutivo"]=str(len(array_ITX)+1)
        ITX_iva["consecutivo_item"]="1"
        ITX_iva["id_impuesto"]="01"
        ITX_iva["porcentaje"]="0.19"
        ITX_iva["base"]=formatear_2Decimales(round(float(iva)/0.19,2))
        ITX_iva["valor_impuesto"]=formatear_2Decimales(float(iva))

        array_ITX.append(ITX_iva)
    
    if float(consumo)>0:
        ITX_consumo=datos_ITX.copy()
        ITX_consumo["consecutivo"]=str(len(array_ITX)+1)
        ITX_consumo["consecutivo_item"]="1"
        ITX_consumo["id_impuesto"]="04"
        ITX_consumo["porcentaje"]="0.04"
        ITX_consumo["base"]=formatear_2Decimales(round(float(consumo)/0.04,2))
        ITX_consumo["valor_impuesto"]=consumo
        array_ITX.append(ITX_consumo)

        
    indicador_exist_unic_eq_finan=False
    if len(array_ITM)==1:
        if array_ITM[0]["descripcion"]=="Interés Equipos financiados":
            array_ITX.clear()
            array_TAX.clear()
            indicador_exist_unic_eq_finan=True

    
    

    

    for itm in array_ITM:
        valor_total+=float(itm["valor_total"])
        itm["valor_recargo_descuento"]=formatear_2Decimales(total_descuento)
        if itm["codigo_item"]=="GPD_M" or itm["codigo_item"]=="IEF_M":
            existe_1160=True
            
            if itm["descripcion"]=="Interés Equipos financiados" and valor_impuesto_coriente_mora>0:
                ITX_interes_corriente_mora=datos_ITX.copy()
                ITX_interes_corriente_mora["consecutivo"]=str(len(array_ITX)+1)
                ITX_interes_corriente_mora["consecutivo_item"]=itm["consecutivo"]
                ITX_interes_corriente_mora["id_impuesto"]="01"
                ITX_interes_corriente_mora["porcentaje"]="0.19"
                ITX_interes_corriente_mora["base"]=formatear_2Decimales(round(valor_impuesto_coriente_mora / 0.19, 2))
                ITX_interes_corriente_mora["valor_impuesto"]=str(valor_impuesto_coriente_mora)
                array_ITX.append(ITX_interes_corriente_mora)
                
                
                


            if itm["descripcion"]=="Gestion prestamo tu desvare" and valor_impuesto_gestion_recarga>0:
                ITX_gestion_recarga=datos_ITX.copy()
                ITX_gestion_recarga["consecutivo"]=str(len(array_ITX)+1)
                ITX_gestion_recarga["consecutivo_item"]=itm["consecutivo"]
                ITX_gestion_recarga["id_impuesto"]="01"
                ITX_gestion_recarga["porcentaje"]="0.19"
                ITX_gestion_recarga["base"]=formatear_2Decimales(round(valor_impuesto_gestion_recarga/0.19,2))
                ITX_gestion_recarga["valor_impuesto"]=str(valor_impuesto_gestion_recarga)
                array_ITX.append(ITX_gestion_recarga)
            
                
        
            
    if abs(total_descuento)>valor_total:
        datos_CAB["total_descuento"]=formatear_2Decimales(abs(valor_total))
        total_descuento=abs(valor_total)*-1


 
    valor_descuento_restante=float(datos_CAB["total_descuento"])
 
    for itm in array_ITM:
         if valor_descuento_restante>0:
            if float(itm["valor_unidad"])>=valor_descuento_restante:
               itm["valor_recargo_descuento"]=formatear_2Decimales(-1*valor_descuento_restante)
               valor_descuento_restante=0
            else:
                 itm["valor_recargo_descuento"]="-"+itm["valor_unidad"]
                 valor_descuento_restante=valor_descuento_restante-float(itm["valor_unidad"])
                 
         else:
              itm["valor_recargo_descuento"]="0.00"
              
         lineas_ITM+=f"\r\n"+"|".join(itm.values())

    
            


    
    #  if existe_tag_1115: datos_CAB["subtotal"]=formatear_2Decimales(subtotal-float(total_equipos_a_credito)+totalIntereses  )
    #datos_CAB["subtotal"]="33338.88
    total_impuesto=0
    for itx in array_ITX:
        
        if itx["porcentaje"]=="0.19": 
            tax_valor_total+=float(itx["base"])
            valor_impuesto_total+=float(itx["valor_impuesto"])

        total_impuesto+=float(itx["valor_impuesto"])


    if (float(iva)>0 and indicador_exist_unic_eq_finan==False or(float(valor_impuesto_coriente_mora)>0 or float(valor_impuesto_gestion_recarga)>0)):
        TAX_iva["valor_impuesto"]=formatear_2Decimales(valor_impuesto_total)
        TAX_iva["base"]=formatear_2Decimales(round(tax_valor_total,2))

        array_TAX.append(TAX_iva)

    if float(consumo)>0:
        TAX_consumo=datos_TAX.copy()
        TAX_consumo["id_impuesto"]="04"
        TAX_consumo["porcentaje"]="0.04"
        TAX_consumo["base"]=formatear_2Decimales(round(float(consumo)/0.04,2))
        TAX_consumo["valor_impuesto"]=formatear_2Decimales(float(consumo))

    
        array_TAX.append(TAX_consumo)

    datos_CAB["total_impuestos"]=formatear_2Decimales(total_impuesto)

    #  if valor_total!=0 and existe_1160==True or existe_tag_1115==True:
    datos_CAB["subtotal"]=formatear_2Decimales(valor_total)
    datos_CAB["total_del_mes"]=formatear_2Decimales(round(float(valor_total)+float(total_impuesto)-abs(float(total_descuento)),2))


    indicador_borrado_consumo=False
    if len(array_ITM)==1:
         if array_ITM[0]["descripcion"]=="Interés Equipos financiados":
              
                for tax in array_TAX:
                     if tax.get("id_impuesto")=="04":
                          array_TAX.remove(tax)
                          consumo="0.00"
              
              

    if len(array_ITM)==0:
    
        servicios_moviles_ITM=datos_ITM.copy()
        servicios_moviles_ITM["consecutivo"]=str(len(array_ITM)+1)
        servicios_moviles_ITM["valor_unidad"]=formatear_2Decimales(1)
        servicios_moviles_ITM["valor_total"]=formatear_2Decimales(1)
        servicios_moviles_ITM["descripcion"]="Servicios móviles"
        servicios_moviles_ITM["codigo_item"]="OSR_M"
        servicios_moviles_ITM["nombre_canal"]="OSR_M"
        servicios_moviles_ITM["valor_recargo_descuento"]="-1"
        

        datos_CAB["total_descuento"]=str(1)
        datos_CAB["subtotal"]=str(1)
        datos_CAB["total_impuestos"]="0.00"
        datos_CAB["total_del_mes"]="0.00"
        lineas_ITM+=f"\r\n"+"|".join(servicios_moviles_ITM.values())

        #TODO: SE ADICIONA PARA VALIDAR CUANDO ES DUMI Y LLEGA EL IMPUESTO AL CONSUMO
        array_ITX.clear()
        array_TAX.clear()



    lineas_itx=""
    for itx in array_ITX:
        lineas_itx+=f"\r\n"+"|".join(itx.values())

    lineas_tax=""
    for tax in array_TAX:
        lineas_tax+=f"\r\n"+"|".join(tax.values())
        
    LINEA_PRC = "|".join(datos_PRC.values())
    LINEA_CAB="|".join(datos_CAB.values())
    LINEA_ADQ="|".join(datos_ADQ.values())
    #LINEA_ADQ="ADQ|1|31|890319193-3|CLIENTE SIESA PRUEBAS|CLIENTE SIESA PRUEBAS|CO|11001|||AV 3AN 26N 83|11001|6013901000||48|R-99-PN|01"

    NOTA_CARGOS_FIJOS=f"NOT|1115-Cargos Fijos:{total_subscription},Impuestos CargosFijos:{tax_total_subscription}"
    NOTA_CONSUMOS=f"NOT|1115-Consumos:{total_usage},Impuestos Consumos:{tax_total_usage}"
    NOTA_OTROS_CONCEPTOS=f"NOT|1115-OtrosConceptos:{formatear_2Decimales(float(total_acces))},Impuestos Otros Conceptos:{formatear_2Decimales(float(tax_total_acces))}"
    NOTA_EQUIPOS_CREDITOS=f"NOT|1115-EquiposaCredito:{total_equipos_a_credito},Impuestos Equipos a Credito:{total_impuesto_equipos_credito}"
    NOTA_SERVICIOS_ADICIONALES=f"NOT|1115-Servicios Adicionales:{formatear_2Decimales(float(total_occs)-count_1330_repo_finan)},Impuestos Servicios Adicionales:{formatear_2Decimales(float(impuesto_total_occs)-count_1330_repo_finan_impuesto)}"
    NOTA_EQUIPOS_FINANCIADOS=f"NOT|1115-EquipoFinanciados:{formatear_2Decimales(round(interes_corriente+interes_mora,2))},Impuestos Equipos Financiados:0.00"
    NOTA_CONSUMOS_IVA=f"NOT|iva:{iva},consumo:{consumo}"
    NOTA_CUSCODE=f"NOT|Custcode:{cust_code}"  
    NOTA_CARTERAS=f"NOT|Carteras ya facturadas:{formatear_2Decimales(count_1330_repo_finan)},Impuestos Carteras:{formatear_2Decimales(count_1330_repo_finan_impuesto)}"   
    factura_final= LINEA_PRC+"\r\n"+LINEA_CAB+"\r\n"+LINEA_ADQ+lineas_ITM+lineas_itx+lineas_tax+"\r\n"+NOTA_CARGOS_FIJOS+"\r\n"+NOTA_CONSUMOS+"\r\n"+NOTA_OTROS_CONCEPTOS+"\r\n"+NOTA_EQUIPOS_CREDITOS+"\r\n"+NOTA_SERVICIOS_ADICIONALES+"\r\n"+NOTA_EQUIPOS_FINANCIADOS+"\r\n"+NOTA_CONSUMOS_IVA+"\r\n"+NOTA_CUSCODE
  
    

   
    if count_1330_repo_finan>0:
         factura_final+=f"\r\n{NOTA_CARTERAS}"
    return factura_final
 except Exception as e:
      print(invoiceNumber)
      print((str(e)))
      return 'error'