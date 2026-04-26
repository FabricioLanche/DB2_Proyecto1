import struct
class TableConfig:
    def __init__(self, table_name, columns_format, column_names):
        self.table_name = table_name
        self.formato_registro = columns_format  # Ejemplo: 'i30si...'
        self.column_names = column_names
        self.tamanho_registro = struct.calcsize(self.formato_registro)

class Record:
    def __init__(self, data_tuple):
        # data_tuple contiene (id, nombre, edad, ..., next_pointer)
        self.values = list(data_tuple)
        self.pk = data_tuple[0]  # Asumimos que la primera columna es la PK
       
        self.next_record = self.pk + 1  # Inicialmente apuntamos al siguiente ID lógico
        self.is_deleted = 0

    def __repr__(self):
        return f"Record(PK={self.pk}, Next={self.next_record})"

class Page:
    page_id = -1
    # records = [Record()]
    def __init__(self, page_id, data):
        self.page_id = page_id
        self.next_page_ptr = -1
        self.data = data

        self.records = []
class Header:
    def __init__(self, t_registros_fisicos, cantidad_auxiliar, primer_registro_logico):
        self.t_registros_fisicos = t_registros_fisicos
        self.cantidad_auxiliar = cantidad_auxiliar
        self.primer_registro_logico = primer_registro_logico
