import pandas as pd
import requests
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import set_with_dataframe
import re



def get_requests():

    paises = ['CHL', 'FRA', 'DEU', 'ITA', 'SWE', 'PRT']
    col = ["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "Display", "Numeric", "Low", "High"]
    rows = []
    for pais in paises:
        url  = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_' +  pais + '.xml'
        get_info = requests.get(url)
        info_pais = ET.fromstring(get_info.content)        
        for child in info_pais:
            if child.find("GHO") is not None:
                gho = child.find("GHO").text
            else: 
                gho = None
            if child.find("COUNTRY") is not None:
                country = child.find("COUNTRY").text
            else: 
                country = None

            if child.find("SEX")  is not None:
                sex = child.find("SEX").text
            else: 
                sex = None
            if child.find("YEAR")  is not None:
                year = child.find("YEAR").text
            else: 
                year = None
            if child.find("GHECAUSES") is not None:
                ghecauses = child.find("GHECAUSES").text
            else: 
                ghecauses = None
            if child.find("AGEGROUP") is not None:
                agegroup = child.find("AGEGROUP").text
            else: 
                agegroup = None
            if child.find("Display") is not None:
                display = child.find("Display").text
            else: 
                display = None
            if child.find("Numeric") is not None:
                numeric = child.find("Numeric").text
                str(numeric)
                numeric = re.sub('[.]',',',numeric)
            else: 
                numeric = None
            if child.find("Low") is not None:
                low = child.find("Low").text
            else: 
                low = None
            if child.find("High") is not None:
                high = child.find("High").text
            else: 
                high = None

            rows.append({"GHO": gho, "COUNTRY": country, "SEX": sex, "YEAR": year, "GHECAUSES": ghecauses, "AGEGROUP": agegroup, "Display": display,
             "Numeric": numeric, "Low": low, "High": high})

    output = pd.DataFrame(rows, columns= col)

    # print(output)
    return output

# MAIN

datos = get_requests()

## PARA PODER IMPORTAR LOS DATOS AL GOOGLE SHEETS SE DEBE TENER EL ARCHIVO JSON CON LAS KEYS, PERO POR TEMAS DE PRIVACIDAD NO LOS INCLUI EN EL ENTREGABLE
## SI LO DESEA PROBAR, PORFAVOR PEDIR ARCHIVO JSON

gc = gspread.service_account(filename='taller-tarea-4-316419-df62ae3cbd5f.json')
sh = gc.open_by_key('1NiCBWV2r0zsFLNF9RHebv1B3fNX2xPUQnYvFDjZblJs')
worksheet = sh.get_worksheet(0)

set_with_dataframe(worksheet, datos)




