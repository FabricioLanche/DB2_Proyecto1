from DBMS.parser.scanner import Scanner
from DBMS.parser.parser import SQLParser
from DBMS.storage.catalog import SystemCatalog
from DBMS.organization.data_structures import TableConfig

class DBMSEngine:
    def __init__(self):
        print("Iniciando DBMS SQL...")
        self.catalog = SystemCatalog()
        print("Catalog cargado exitosamente!")

    def execute_query(self, sql_string):
        try:
            # Scanner y Parser
            scanner = Scanner(sql_string)
            tokens = scanner.tokenize()
            parser = SQLParser(tokens)
            ast_array = parser.parse()

            # Ejecutar SQL
            for ast in ast_array:
                self._route_statement(ast)
                
        except Exception as e:
            print(f"Error: {e}")

    def _route_statement(self, ast):
        action = ast["action"]
        if action == "CREATE": self._execute_create(ast)
        elif action == "INSERT": self._execute_insert(ast)
        elif action == "SELECT": self._execute_select(ast)
        elif action == "DELETE": self._execute_delete(ast)
 
    def _generar_formato_struct(self, columns):
        formato = ""
        for col in columns:
            tipo = col["tipo"]
            if tipo == "INT": formato += "i"
            elif tipo == "DOUBLE": formato += "d"
            elif tipo == "POINT": formato += "dd"
            elif tipo.startswith("VARCHAR"):
                size = tipo.split("(")[1].replace(")", "")
                formato += f"{size}s"
        return formato

    # --- Lógica Semántica ---
    def _execute_create(self, ast):
        table_name = ast["table"]
        columns = ast["columns"]
        filepath = ast["file"]
        
        pk_count = 0
        for col in columns:
            if col.get("primary_key"):
                pk_count += 1
                
            tech = col.get("index_tech")
            tipo = col.get("tipo")
            
            if tech:
                if tech == "SEQUENTIAL":
                    if tipo not in ["INT", "DOUBLE"]:
                        raise Exception(f"Error Semántico: SEQUENTIAL solo soporta columnas numéricas. La columna '{col['nombre']}' es {tipo}.")
                elif tech == "RTREE" and tipo != "POINT":
                        raise Exception(f"Error Semántico: RTREE es un índice espacial y solo soporta el tipo POINT. La columna '{col['nombre']}' es {tipo}.")
                if tipo == "POINT" and tech != "RTREE":
                    raise Exception(f"Error Semántico: El tipo POINT no puede ser indexado usando {tech}. Debe usar RTREE.")

        if pk_count > 1:
            raise Exception(f"Error Semántico: Múltiples PRIMARY KEY detectadas en '{table_name}'.")    

        
        self.catalog.create_table(table_name, columns)
        print(f"CREATE: Tabla '{table_name}' creada con exito!")

        if filepath:
            print(f" Implementar Heapfile se encarga de la carga de datos de '{filepath}'")

    def _execute_insert(self, ast):
        table_name = ast["table"]
        values = ast["values"]
        
        # Existencia de la tabla
        esquema = self.catalog.get_table_schema(table_name)
        if len(values) != len(esquema):
            raise Exception(f"INSERT fallido: Se esperaban {len(esquema)} valores.")
        

        # Type Checking
        for val, col in zip(values, esquema):
            expected_type = col["tipo"]
            col_name = col["nombre"]
            
            if expected_type == "INT" and not isinstance(val, int):
                raise Exception(f"Type Error: La columna '{col_name}' espera un INT.")
            
            elif expected_type == "DOUBLE" and not isinstance(val, (int, float)):
                raise Exception(f"Type Error: La columna '{col_name}' espera un DOUBLE.")
                
            elif expected_type.startswith("VARCHAR"):
                if not isinstance(val, str):
                    raise Exception(f"Type Error: La columna '{col_name}' espera un VARCHAR (String).")
                # Validar que el string no exceda el tamaño del VARCHAR
                max_len = int(expected_type.split("(")[1].replace(")", ""))
                if len(val) > max_len:
                    raise Exception(f"Type Error: El valor '{val}' excede el límite de VARCHAR({max_len}).")
            elif expected_type == "POINT":
                # (10.5, 20.0)
                if not isinstance(val, (tuple, list)) or len(val) != 2:
                    raise Exception(f"Type Error: La columna '{col_name}' espera un POINT con formato (x, y).")
                if not all(isinstance(coord, (int, float)) for coord in val):
                    raise Exception(f"Type Error: Las coordenadas del POINT '{col_name}' deben ser numéricas.")

        formato_binario = self._generar_formato_struct(esquema)

        
        valores_aplanados = []
        for val in values:
            if isinstance(val, tuple): # Si es un POINT (tupla), sacamos X e Y por separado
                valores_aplanados.extend(val)
            else:
                valores_aplanados.append(val)
        

        print(f"\n[USAR HEAP FILE] -> Ejecutar insert() en disco para '{table_name}'.")
        print(f"   * Datos a guardar: {values}")
        print(f"   * Datos aplanados (para struct): {valores_aplanados}") # hay que pensar en la conexion como seria para cada indice
        print(f"   * Formato struct a usar: '{formato_binario}'")
        
        # placeHolder de return de seq file:
        puntero_Heap_File = {"page_id": 0, "offset": 50}

        # retornar solo los valores de la columna indexada 

        
        # actualizar indices de columas
        for col in esquema:
            if col["index_tech"] == "SEQUENTIAL":
                print(f"[HOOK INDEX] -> Insertar en Sequential File de columna '{col['nombre']}'. Clave a enviar: el valor correspondiente, Puntero: {puntero_Heap_File}")
            elif col["index_tech"] == "BTREE":
                print(f"[HOOK INDEX] -> Insertar en B-Tree de columna '{col['nombre']}'. Clave a enviar: el valor correspondiente, Puntero: {puntero_Heap_File}")
            elif col["index_tech"] == "HASH":
                print(f"[HOOK INDEX] -> Insertar Clave '{col['nombre']}' en Extendible Hash. Puntero: {puntero_Heap_File}")
            elif col["index_tech"] == "RTREE":
                print(f"[HOOK INDEX] -> Insertar Punto '{col['nombre']}' en R-Tree espacial. Puntero: {puntero_Heap_File}")

    def _execute_select(self, ast):
        table_name = ast["table"]
        esquema = self.catalog.get_table_schema(table_name)
        print(f"\n[HOOK] -> Ejecutar búsqueda en índices o Sequential File para la tabla '{table_name}'.")

    def _execute_delete(self, ast):
        table_name = ast["table"]
        esquema = self.catalog.get_table_schema(table_name)
        print(f"\n[HOOK] -> Buscar registro en índices y marcar 'is_deleted = 1' en Sequential File.")