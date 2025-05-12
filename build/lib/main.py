import pandas as pd
from database import DataBase 
import os
import traceback 

def main():
    
    ruta_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'cvs', 'enfermzoonoticas.csv')
    
    print(f"Intentando leer CSV desde: {ruta_csv}")

    try:
        df = pd.read_csv(ruta_csv, encoding='latin1')
        print("\n***** 1. Datos iniciales leídos del CSV *****")
        print(df.head())
        print(f"Forma del DataFrame leído del CSV: {df.shape}")

        if df.empty:
            print("[INFO] El archivo CSV está vacío o no se pudo leer correctamente. Saliendo.")
            return

        
        nombre_tabla = "enfermedades_zoonoticas"
        database = DataBase() 

        print(f"\n***** 2. Insertando/Reemplazando datos en la tabla '{nombre_tabla}' *****")
        database.insert_data(df, nombre_tabla)
        
        print(f"\n***** 3. Leyendo datos de la BD DESPUÉS de la inserción *****")
        df_despues_insert = database.read_data(nombre_tabla)
        if df_despues_insert is not None and not df_despues_insert.empty:
            print(df_despues_insert.head())
            print(f"Forma de los datos en BD después de insertar: {df_despues_insert.shape}")
        else:
            print(f"[INFO] No se encontraron datos en la tabla '{nombre_tabla}' después de la inserción o hubo un error.")
           
            return


        # --- SECCIÓN DE ACTUALIZACIÓN ---
        print(f"\n***** 4. Intentando ACTUALIZAR un registro *****")
       
        
        codigo_para_actualizar = 15
        columna_a_actualizar = "localidad" 
        nuevo_valor = "Esta es una localidad actualizada."

        
        if df_despues_insert is not None and not df_despues_insert[df_despues_insert["codigo"] == codigo_para_actualizar].empty:
            print(f"Registro ANTES de la actualización (codigo={codigo_para_actualizar}):")
            print(df_despues_insert[df_despues_insert["codigo"] == codigo_para_actualizar])

            database.update_data(
                nom_table=nombre_tabla,
                set_column=columna_a_actualizar,
                set_value=nuevo_valor,
                where_column="codigo",
                where_value=codigo_para_actualizar
            )

            print(f"\n***** 5. Leyendo datos de la BD DESPUÉS de la ACTUALIZACIÓN *****")
            df_despues_actualizar = database.read_data(nombre_tabla)
            if df_despues_actualizar is not None and not df_despues_actualizar.empty:
                print(f"Registro DESPUÉS de la actualización (codigo={codigo_para_actualizar}):")
                print(df_despues_actualizar[df_despues_actualizar["codigo"] == codigo_para_actualizar].head())
                print(f"Forma de los datos en BD DESPUÉS de actualizar: {df_despues_actualizar.shape}")
            else:
                print(f"[INFO] No se pudieron leer datos de la tabla '{nombre_tabla}' después de la actualización.")
        else:
            print(f"[ADVERTENCIA] No se encontró un registro con codigo={codigo_para_actualizar} para actualizar, o la tabla está vacía.")
            df_despues_actualizar = df_despues_insert 

        # --- SECCIÓN DE ELIMINACIÓN ---
        print(f"\n***** 6. Intentando ELIMINAR un registro *****")
       
        columna_para_eliminar = "codigo" 
        valor_a_eliminar = 12            

       
        df_actual_para_eliminar_check = df_despues_actualizar if df_despues_actualizar is not None else df_despues_insert

        if columna_para_eliminar not in df.columns:
            print(f"[ADVERTENCIA] La columna '{columna_para_eliminar}' no existe en el CSV. No se puede eliminar.")
        elif df_actual_para_eliminar_check is None or df_actual_para_eliminar_check.empty:
            print(f"[ADVERTENCIA] No hay datos en la tabla '{nombre_tabla}' para verificar el valor a eliminar.")
        else:
            existe_valor = valor_a_eliminar in df_actual_para_eliminar_check[columna_para_eliminar].unique()
            if not existe_valor:
                 print(f"[ADVERTENCIA] El valor '{valor_a_eliminar}' no se encuentra en la columna '{columna_para_eliminar}' de la tabla '{nombre_tabla}' ANTES de la eliminación.")

            database.delete_data(
                nom_table=nombre_tabla,
                where_column=columna_para_eliminar,
                where_value=valor_a_eliminar
            )

            print(f"\n***** 7. Leyendo datos de la BD DESPUÉS de la ELIMINACIÓN *****")
            df_despues_eliminar = database.read_data(nombre_tabla)
            if df_despues_eliminar is not None and not df_despues_eliminar.empty:
                print(df_despues_eliminar.head())
                print(f"Forma de los datos en BD DESPUÉS de eliminar: {df_despues_eliminar.shape}")
                fila_eliminada = df_despues_eliminar[df_despues_eliminar[columna_para_eliminar] == valor_a_eliminar]
                if fila_eliminada.empty:
                    print(f"[OK] Confirmado: La fila con {columna_para_eliminar} = {valor_a_eliminar} ha sido eliminada de la BD.")
                else:
                    print(f"[ALERTA] La fila con {columna_para_eliminar} = {valor_a_eliminar} TODAVÍA existe en la BD.")
            elif df_despues_eliminar is not None and df_despues_eliminar.empty:
                print(f"[INFO] La tabla '{nombre_tabla}' está vacía después de la eliminación.")
            else:
                print(f"[INFO] No se pudieron leer datos de la tabla '{nombre_tabla}' después de la eliminación o la tabla no existe.")

    except FileNotFoundError:
        print(f"[ERROR] Archivo no encontrado: {ruta_csv}")
    except pd.errors.EmptyDataError:
        print(f"[ERROR] El archivo CSV '{ruta_csv}' está vacío o no tiene datos.")
    except KeyError as e:
        print(f"[ERROR] Error de columna: Es posible que la columna '{e}' no exista en el CSV o en la tabla. Revisa los nombres.")
        traceback.print_exc() 
    except Exception as e:
        print(f"[ERROR FATAL] Error inesperado al ejecutar el script main: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    if os.name == 'nt':
        try:
            os.system('chcp 65001 > nul')           
        except Exception as e_chcp:
            pass 
    main()