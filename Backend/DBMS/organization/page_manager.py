#Capa 1: Acceso a Disco

import os

class PageManager:
    PAGE_SIZE = 4096  # Tamaño estándar de 4 KB solicitado

    def __init__(self, db_filename):
        self.db_filename = db_filename
        self.read_count = 0
        self.write_count = 0

        #Cache: ultima pagina accedida
        self.last_page_id_loaded = -1
        self.last_page_data = None

        # Crea el archivo si no existe
        if not os.path.exists(db_filename):
          with open(db_filename, 'wb') as f:
              pass

    def read_page(self, page_id):
        if page_id == self.last_page_id_loaded:
            return self.last_page_data
        self.read_count += 1
        offset = page_id * self.PAGE_SIZE
        with open(self.db_filename, 'rb') as f:
            f.seek(offset)
            data_leida = f.read(self.PAGE_SIZE)
        #Rellenamos con ceros si el contenido de la pagina no llega a ser 4KB
        if len(data_leida) < self.PAGE_SIZE:
           data_leida = data_leida.ljust(self.PAGE_SIZE, b'\x00')
        self.last_page_id_loaded = page_id
        self.last_page_data = data_leida
        return data_leida

    def write_page(self, page_id, data):
        self.write_count += 1
        offset = page_id * self.PAGE_SIZE
        with open(self.db_filename, 'r+b') as f:
            f.seek(offset)
            f.write(data.ljust(self.PAGE_SIZE, b'\x00'))
        if page_id == self.last_page_id_loaded:
          self.last_page_id_loaded = -1
          self.last_page_data = None

    def reset_counters(self):
        """Reinicia el contador para una nueva medición."""
        self.read_count = 0
        self.write_count = 0

    def get_stats(self):
        """Retorna las métricas actuales."""
        return {
            "reads": self.read_count,
            "writes": self.write_count,
            "total": self.read_count + self.write_count
        }
    def allocate_new_page(self) -> int:
     with open(self.db_filename, 'r+b') as f:
        f.seek(0, 2)                          # Va al final del archivo
        eof_offset = f.tell()                 # Posición actual = tamaño total
        new_page_id = eof_offset // self.PAGE_SIZE  # ID de la nueva página
        f.write(b'\x00' * self.PAGE_SIZE)     # Escribe 4096 bytes vacíos
     self.write_count += 1                     # Cuenta como acceso de escritura
     return new_page_id