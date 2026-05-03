import struct
import csv
from typing import List, Tuple, Optional
from .page_manager import PageManager
from .data_structures import TableConfig, Record


class HeapFile:
    # Formato de la página 0 (metadata)
    # first_free_rid_page (int32), first_free_rid_slot (int32), last_page_id (int32), total_records (int32)
    _METADATA_FORMAT = '<iiii'
    _METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)
    
    # Formato de la cabecera de página de datos: record_count (int16)
    _PAGE_HEADER_FORMAT = '<H'
    _PAGE_HEADER_SIZE = struct.calcsize(_PAGE_HEADER_FORMAT)
    
    def __init__(self, filename: str, config: TableConfig, page_manager: Optional[PageManager] = None):
        self.filename = filename
        self.config = config
        self.pm = page_manager or PageManager(filename)
        
        # Calcula cuántos registros caben por página
        usable_space = self.pm.PAGE_SIZE - self._PAGE_HEADER_SIZE
        self.records_per_page = usable_space // config.get_data_size()
        
        # Inicializa o carga metadata de la página 0
        self._init_metadata()
    
    def _init_metadata(self) -> None:
        page0 = self.pm.read_page(0)
        
        if page0[:self._METADATA_SIZE] == b'\x00' * self._METADATA_SIZE:
            self.first_free_rid = (-1, -1)  # Sin huecos inicialmente
            self.last_page_id = 0
            self.total_records = 0 # Total de registros activos
            self._persist_metadata()
        else:
            # Cargar metadata existente
            data = struct.unpack(self._METADATA_FORMAT, page0[:self._METADATA_SIZE])
            self.first_free_rid = (data[0], data[1])
            self.last_page_id = data[2]
            self.total_records = data[3]
    
    def _persist_metadata(self) -> None:
        # Guarda la metadata en la página 0 usando PageManager
        # Solo llamar en momentos clave (flush, init)."""
        page0 = bytearray(self.pm.read_page(0))
        metadata = struct.pack(
            self._METADATA_FORMAT,
            self.first_free_rid[0],  # page_id 
            self.first_free_rid[1],  # slot_id 
            self.last_page_id,
            self.total_records
        )
        page0[:self._METADATA_SIZE] = metadata
        self.pm.write_page(0, bytes(page0))
    
    def flush_metadata(self) -> None:
        self._persist_metadata()
    
    def insert(self, record: Record) -> Tuple[int, int]:
        record_bytes = record.to_bytes(self.config)
        
        # Caso 1: Hay "huecos" disponibles
        if self.first_free_rid != (-1, -1):
            page_id, slot_id = self.first_free_rid
            page = bytearray(self.pm.read_page(page_id))
            
            slot_offset = self._slot_offset(slot_id)  # ← Calcula el offset correcto

            # El hueco almacena el siguiente RID disponible en sus primeros 8 bytes
            if len(page) >= slot_offset + 8:
                next_page, next_slot = struct.unpack('<ii', page[slot_offset:slot_offset+8])
                self.first_free_rid = (next_page, next_slot)
            else:
                self.first_free_rid = (-1, -1)
            self.total_records += 1
            # Sobrescribe el slot con el nuevo registro
            self._write_record_to_slot(page, slot_id, record_bytes)
            self.pm.write_page(page_id, bytes(page))
            # Metadata se mantiene en RAM; se persiste en flush()
            return (page_id, slot_id)
        
        # Caso 2: No hay huecos
        page_id = self.last_page_id
        page = bytearray(self.pm.read_page(page_id))
        record_count = self._read_page_header(page)
        
        # Si la página actual está llena, asigna una nueva
        if record_count >= self.records_per_page:
            page_id = self.pm.allocate_new_page()
            self.last_page_id = page_id
            page = bytearray(self.pm.read_page(page_id))  # ← Leer desde PM
            record_count = self._read_page_header(page)   # ← Obtener count real
        
        slot_id = record_count
        self._write_record_to_slot(page, slot_id, record_bytes)
        record_count += 1
        self._write_page_header(page, record_count)
        
        self.pm.write_page(page_id, bytes(page))
        self.total_records += 1
        # Metadata se mantiene en RAM; se persiste en flush()
        
        return (page_id, slot_id)
    
    def search(self, rid: Tuple[int, int]) -> Optional[Record]:
        page_id, slot_id = rid
        page = self.pm.read_page(page_id)
        record_count = self._read_page_header(page)
        
        if slot_id >= record_count:
            return None # Slot fuera de rango -> No existe el registro
        
        record_bytes = self._read_record_from_slot(page, slot_id)
        data_tuple = struct.unpack(self.config.data_format, record_bytes)
        return Record(data_tuple, self.config)
    
    #Util para busqueda por rango tras obtener lista de RIDs proporcionada por los indices
    def get_batch(self, rid_list: List[Tuple[int, int]]) -> List[Record]:
        #Recupera múltiples registros agrupando por página
        if not rid_list:
            return []
        
        # Agrupar por page_id
        by_page = {}
        for page_id, slot_id in rid_list:
            if page_id not in by_page:
                by_page[page_id] = []
            by_page[page_id].append(slot_id)
        
        # Ordenar páginas para lectura secuencial
        sorted_pages = sorted(by_page.keys())
        
        records = []
        for page_id in sorted_pages:
            page = self.pm.read_page(page_id)
            for slot_id in by_page[page_id]:
                record_bytes = self._read_record_from_slot(page, slot_id)
                data_tuple = struct.unpack(self.config.data_format, record_bytes)
                records.append(Record(data_tuple, self.config))
        
        return records
    
    def delete(self, rid: Tuple[int, int]) -> bool:
        page_id, slot_id = rid
        page = bytearray(self.pm.read_page(page_id))
        record_count = self._read_page_header(page)
        
        if slot_id >= record_count:
            return False # Slot fuera de rango -> No existe el registro
        
        # Guarda el RID actual del first_free_rid en los primeros 8 bytes
        next_rid_bytes = struct.pack('<ii', self.first_free_rid[0], self.first_free_rid[1])
        
        # Rellena el slot con el siguiente RID disponible
        slot_offset = self._slot_offset(slot_id)
        page[slot_offset:slot_offset + 8] = next_rid_bytes
        
        # Actualiza el header de la lista enlazada de huecos
        self.first_free_rid = (page_id, slot_id)
        
        self.pm.write_page(page_id, bytes(page))
        self.total_records = max(0, self.total_records - 1)
        # Metadata se mantiene en RAM; se persiste en flush()
        return True
    
    def load_from_csv(self, csv_path: str) -> List[Tuple[int, int]]:
        rids = []

        # Orden de columnas según column_map para garantizar consistencia con el formato de datos
        col_order = sorted(self.config.column_map.items(), key=lambda x: x[1])
        col_names = [name for name, _ in col_order]
        
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Construye la tupla en el orden correcto
                values = []
                for col_name in col_names:
                    col_fmt = self.config.get_column_format(col_name)
                    raw     = row[col_name].strip()
                    values.append(self._cast_value(raw, col_fmt))
                
                data_tuple = tuple(values)
                record     = Record(data_tuple, self.config)
                rid        = self.insert(record) # Se inserta en el heap file y se obtiene el RID
                rids.append(rid)
        
        return rids

    def _cast_value(self, raw: str, fmt: str):
        fmt_clean = fmt.lstrip('<>=!')
        if fmt_clean[-1] in ('i', 'l', 'q', 'h', 'b', 'I', 'L', 'Q', 'H', 'B'):
            return int(raw)
        elif fmt_clean[-1] in ('f', 'd'):
            return float(raw)
        elif fmt_clean[-1] == 's':
            size = int(fmt_clean[:-1]) if fmt_clean[:-1] else 1
            return raw.encode('utf-8')[:size].ljust(size, b'\x00')
        return raw
    
    # ============ Métodos internos (helpers) ============
    
    def _slot_offset(self, slot_id: int) -> int:
        # Calcula el offset para un slot 
        return self._PAGE_HEADER_SIZE + (slot_id * self.config.get_data_size())
    
    def _read_page_header(self, page: bytes) -> int:
        #Lee el conteo de registros activos de una página
        return struct.unpack(self._PAGE_HEADER_FORMAT, page[:self._PAGE_HEADER_SIZE])[0]
    
    def _write_page_header(self, page: bytearray, record_count: int) -> None:
        # Escribe el conteo de registros en la cabecera de página.
        header = struct.pack(self._PAGE_HEADER_FORMAT, record_count)
        page[:self._PAGE_HEADER_SIZE] = header
    
    def _read_record_from_slot(self, page: bytes, slot_id: int) -> bytes:
        # Lee los bytes de un registro
        offset = self._slot_offset(slot_id)
        size = self.config.get_data_size()
        return page[offset:offset + size]
    
    def _write_record_to_slot(self, page: bytearray, slot_id: int, record_bytes: bytes) -> None:
        offset = self._slot_offset(slot_id)
        page[offset:offset + len(record_bytes)] = record_bytes
