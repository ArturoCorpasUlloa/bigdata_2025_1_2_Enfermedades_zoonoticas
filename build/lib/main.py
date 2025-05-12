import pandas as pd
from database import DataBase

def main():
    # Leer el archivo CSV
    try:
        ruta_csv = 'src/static/cvs/enfermzoonoticas.csv'
        df = pd.read_csv(ruta_csv, encoding='latin1')
        print("***** Impresión de los primeros datos obtenidos *****")
        print(df.head())

        # Insertar en la base de datos
        nombre_tabla = "enfermedades_zoonoticas"
        database = DataBase()
        database.insert_data(df, nombre_tabla)
        print(f"***** Datos insertados en la tabla '{nombre_tabla}' correctamente *****")

    except FileNotFoundError:
        print(f"⚠️ Archivo no encontrado: {ruta_csv}")
    except Exception as e:
        print(f"❌ Error inesperado al ejecutar el script: {e}")
    
    # Leer los datos para confirmar que se insertaron
    print("\n***** Datos guardados en la base de datos *****")
    df_guardado = database.read_data(nombre_tabla)
    print(df_guardado.head())
        


# Asegúrate de tener la sintaxis correcta del main
if __name__ == "__main__":
    main()
