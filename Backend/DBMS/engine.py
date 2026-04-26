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
            elif tipo.startswith("VARCHAR"):
                size = tipo.split("(")[1].replace(")", "")
                formato += f"{size}s"
        return formato

    # --- Lógica Semántica ---
    def _execute_create(self, ast):
        table_name = ast["table"]
        columns = ast["columns"]
        filepath = ast["file"]
        
        # Validar Llave Primaria
        pk_count = sum(1 for col in columns if col.get("primary_key"))
        if pk_count > 1:
            raise Exception(f"Multiples PRIMARY KEY detectadas en '{table_name}'.")
            
        self.catalog.create_table(table_name, columns)
        print(f"CREATE: Tabla '{table_name}' creada con exito!")

        if filepath:
            print(f" Falta implementar en engine.py cargar datos de '{filepath}'")

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

        formato_binario = self._generar_formato_struct(esquema)
        
        print(f"\n[USAR SEQUENTIAL FILE] -> Ejecutar insert() en disco para '{table_name}'.")
        print(f"   * Datos a guardar: {values}")
        print(f"   * Formato struct a usar: '{formato_binario}'")
        
        # placeHolder de return de seq file:
        puntero_seq_file_placeholder = {"page_id": 0, "offset": 50}
        
        # actualizar indices de columas
        for col in esquema:
            if col["index_tech"] == "BTREE":
                print(f"[HOOK INDEX] -> Insertar en B-Tree de columna '{col['nombre']}'. Clave a enviar: el valor correspondiente, Puntero: {puntero_seq_file_placeholder}")
            elif col["index_tech"] == "HASH":
                print(f"[HOOK INDEX] -> Insertar Clave '{col['nombre']}' en Extendible Hash. Puntero: {puntero_seq_file_placeholder}")
            elif col["index_tech"] == "RTREE":
                print(f"[HOOK INDEX] -> Insertar Punto '{col['nombre']}' en R-Tree espacial. Puntero: {puntero_seq_file_placeholder}")

    def _execute_select(self, ast):
        table_name = ast["table"]
        esquema = self.catalog.get_table_schema(table_name)
        print(f"\n[HOOK] -> Ejecutar búsqueda en índices o Sequential File para la tabla '{table_name}'.")

    def _execute_delete(self, ast):
        table_name = ast["table"]
        esquema = self.catalog.get_table_schema(table_name)
        print(f"\n[HOOK] -> Buscar registro en índices y marcar 'is_deleted = 1' en Sequential File.")