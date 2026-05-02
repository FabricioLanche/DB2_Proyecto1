import struct
import os
import math
from typing import List, Tuple, Optional
from .page_manager import PageManager
from Backend.DBMS.organization import page_manager

class SequentialIndex:
    # Formato de metadata de página 0:
    # n_main, k_aux, k_limit, first_logical_pos, last_main_page
    _METADATA_FORMAT = 'iiiii'
    _METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)
    
    def __init__(self, filename: str, page_manager: Optional[PageManager] = None, pk_format: str='i'):
        self.filename = filename
        self.pm = page_manager or PageManager(filename)
            # Formato del nodo del índice: PK, rid_page, rid_slot, next_pos
        self.pk_format = pk_format 
        self.pk_size = struct.calcsize(pk_format)  # Ej: 15
        self.pk_is_str = pk_format.endswith('s')
        
        # NODE = PK + rid_page + rid_slot + next_pos
        self.NODE_FORMAT = f'{pk_format}iii'  # Dinámico
        self.NODE_SIZE = struct.calcsize(self.NODE_FORMAT)  # Dinámico
        
        
        # Calcula cuántas entradas caben por página
        usable_space = self.pm.PAGE_SIZE - 4  # 4 bytes de header (record_count)
        self.entries_per_page = usable_space // self.NODE_SIZE
        
        self._init_metadata()
        
        # Carga el sparse_index en RAM 
        self.sparse_index = self._build_sparse_index()
    
    def _init_metadata(self) -> None:
        page0 = self.pm.read_page(0)
        
        if page0[:self._METADATA_SIZE] == b'\x00' * self._METADATA_SIZE:
            #Crear metadata inicial
            self.n_main = 0  
            self.k_aux = 0 
            self.k_limit = 1 
            self.first_logical_pos = -1
            self.last_main_page = 0  
            self._persist_metadata()
        else:
            # Cargar metadata existente
            data = struct.unpack(self._METADATA_FORMAT, page0[:self._METADATA_SIZE])
            self.n_main = data[0]
            self.k_aux = data[1]
            self.k_limit = data[2]
            self.first_logical_pos = data[3]
            self.last_main_page = data[4]
    
    def _persist_metadata(self) -> None:
        # Guarda metadata en página 0
        page0 = bytearray(self.pm.read_page(0))
        metadata = struct.pack(
            self._METADATA_FORMAT,
            self.n_main,
            self.k_aux,
            self.k_limit,
            self.first_logical_pos,
            self.last_main_page
        )
        page0[:self._METADATA_SIZE] = metadata

        # Pack pk_format (máx 16 bytes para el string)
        pk_fmt_bytes = self.pk_format.encode('ascii').ljust(16, b'\x00')
        page0[self._METADATA_SIZE:self._METADATA_SIZE+16] = pk_fmt_bytes
    
        self.pm.write_page(0, bytes(page0))
    
    def _build_sparse_index(self) -> List[Tuple[int, int]]:
        sparse = []
        
        if self.last_main_page < 1:
            return sparse
        
        for page_id in range(1, self.last_main_page + 1):
            page = self.pm.read_page(page_id)
            entry_count = self._read_page_header(page)
            
            if entry_count > 0:
                # Lee la primera entrada de la página
                first_entry = self._read_by_RID(page, 0)
                pk = first_entry[0]
                sparse.append((pk, page_id))
        
        return sparse
    
    def search_rid(self, pk: int) -> Optional[Tuple[int, int]]:
        """Busca el RID asociado a una PK.
        Estrategia:
        1. Búsqueda binaria en sparse_index para identificar página principal.
        2. Escaneo lineal en esa página.
        3. Si no encuentra, sigue el next_pos hacia el área auxiliar.
        """
        if not self.sparse_index:
            return None
        
        # Búsqueda binaria para encontrar la página candidata
        left, right = 0, len(self.sparse_index) - 1
        page_idx = 0
        
        while left <= right:
            mid = (left + right) // 2
            mid_pk, mid_page = self.sparse_index[mid]
            if mid_pk <= pk:
                page_idx = mid
                left = mid + 1
            else:
                right = mid - 1
        
        candidate_page = self.sparse_index[page_idx][1]
        
        # Escanea la página principal
        page = self.pm.read_page(candidate_page)
        entry_count = self._read_page_header(page)
        
        for slot_idx in range(entry_count):
            pk_entry, rid_p, rid_s, next_pos = self._read_by_RID(page, slot_idx)
            
            if pk_entry == pk:
                return (rid_p, rid_s)
            
            if pk_entry > pk:
                # PK más grande encontrado, sigue el next_pos si lo hay
                if next_pos != -1:
                    return self._follow_chain(pk, next_pos)
                return None
        
        # Puede haber más en el área auxiliar, sigue el last entry's next_pos
        if entry_count > 0:
            _, _, _, last_next_pos = self._read_by_RID(page, entry_count - 1)
            if last_next_pos != -1:
                return self._follow_chain(pk, last_next_pos)
        
        return None
    
    def range_search_rids(self, pk1: int, pk2: int) -> List[Tuple[int, int]]:
        result = []
        
        # Encuentra el primer registro >= pk1
        current_rid = self._find_first_greater_equal(pk1)
        if not current_rid:
            return []
        
        # Recorre la cadena lógica hasta pk > pk2
        visited = set()
        
        while current_rid:
            page_id, slot_id = current_rid
            
            # Prevención de ciclos
            pos_key = (page_id, slot_id)
            if pos_key in visited:
                break
            visited.add(pos_key)
            
            page = self.pm.read_page(page_id)
            pk_entry, rid_p, rid_s, next_pos = self._read_by_RID(page, slot_id)
            
            if pk_entry < pk1:
                current_rid = self._follow_next_pos(next_pos)
                continue
            
            if pk_entry > pk2:
                break
            
            result.append((rid_p, rid_s))
            current_rid = self._follow_next_pos(next_pos)
        
        return result
    
    def add(self, pk: int, rid: Tuple[int, int]) -> None:
        rid_page, rid_slot = rid
        
        # Localiza el predecesor lógico y su sucesor
        pred_pos, succ_pos = self._find_predecessor(pk)
        
        # Inserta en área auxiliar
        new_pos = self._allocate_entry_in_aux()
        self._write_entry_at_pos(new_pos, pk, rid_page, rid_slot, succ_pos)
        
        if pred_pos == -1:
            self.first_logical_pos = new_pos
        else:
            # Actualiza predecesor
            page_id, slot_id = self._pos_to_page_slot(pred_pos)
            page = bytearray(self.pm.read_page(page_id))
            pk_pred, rp, rs, _ = self._read_by_RID(page, slot_id)
            self._write_entry_on_page(page, slot_id, pk_pred, rp, rs, new_pos)
            self.pm.write_page(page_id, bytes(page))
        
        self.k_aux += 1
        self.n_main += 1
        self._persist_metadata()
        
        if self.k_aux > self.k_limit:
            self.reconstruct()
    
    def remove(self, pk: int) -> Optional[Tuple[int, int]]:
        pred_pos, curr_pos = self._find_entry_and_predecessor(pk)
        
        if curr_pos == -1:
            return None
        
        curr_page_id, curr_slot_id = self._pos_to_page_slot(curr_pos)
        page = bytearray(self.pm.read_page(curr_page_id))
        
        pk_curr, rid_p, rid_s, next_pos = self._read_by_RID(page, curr_slot_id)
        
        if pred_pos == -1:
            # curr era el primero
            self.first_logical_pos = next_pos
        else:
            pred_page_id, pred_slot_id = self._pos_to_page_slot(pred_pos)
            pred_page = bytearray(self.pm.read_page(pred_page_id))
            pk_pred, rp, rs, _ = self._read_by_RID(pred_page, pred_slot_id)
            self._write_entry_on_page(pred_page, pred_slot_id, pk_pred, rp, rs, next_pos)
            self.pm.write_page(pred_page_id, bytes(pred_page))
        
        #  marcamos como eliminado
        self._write_entry_on_page(page, curr_slot_id, self._tombstone_pk(), -1, -1, -1)
        self.pm.write_page(curr_page_id, bytes(page))
        
        self.n_main = max(0, self.n_main - 1)
        self._persist_metadata()
        
        return (rid_p, rid_s)
    
    def reconstruct(self) -> None:
        # FASE 1 — Cargar auxiliares en RAM
        # k_aux ≤ ceil(log₂ n)  →  siempre pequeño
        # I/O: k_aux lecturas 
        aux_entries = [] 

        for page_id in range(self.last_main_page + 1, self.last_main_page + 1 + self.k_aux + 1):
            try:
                page = self.pm.read_page(page_id)
            except Exception:
                break
            entry_count = self._read_page_header(page)
            for slot in range(entry_count):
                pk, rp, rs, _ = self._read_by_RID(page, slot)
                if not pk==self._tombstone_pk():
                    aux_entries.append((pk, rp, rs))

        aux_entries.sort(key=lambda x: x[0])
        aux_ptr = 0

        # FASE 2 — Merge secuencial: main (disco) + aux (RAM)
        # Main se lee página por página → máximo beneficio del caché de 1 página
        # I/O: P_main lecturas secuenciales + P_out escrituras secuenciales

        temp_filename = self.filename + ".tmp"
        temp_pm = PageManager(temp_filename)

        out_page_id  = 1
        out_page     = bytearray(b'\x00' * temp_pm.PAGE_SIZE)
        out_slot     = 0
        total_valid  = 0
        new_sparse   = []

        def _emit(pk, rp, rs):
            nonlocal out_page_id, out_page, out_slot, total_valid

            if out_slot == 0:                          # Primera entrada de página
                new_sparse.append((pk, out_page_id))  # Actualiza sparse_index

            # next_pos = -1 temporal; se corrige en Fase 3
            self._write_entry_on_page(out_page, out_slot, pk, rp, rs, -1)
            out_slot   += 1
            total_valid += 1

            if out_slot >= self.entries_per_page:
                self._write_page_header(out_page, out_slot)
                temp_pm.write_page(out_page_id, bytes(out_page))
                out_page_id += 1
                out_page  = bytearray(b'\x00' * temp_pm.PAGE_SIZE)
                out_slot  = 0

        # Escaneo secuencial del área principal — la caché de PageManager
        for page_id in range(1, self.last_main_page + 1):
            page        = self.pm.read_page(page_id)   # HIT en slots siguientes
            entry_count = self._read_page_header(page)

            for slot in range(entry_count):
                pk_m, rp_m, rs_m, _ = self._read_by_RID(page, slot)

                if pk_m == self._tombstone_pk():
                    continue

                # Intercala todas las aux que van ANTES de pk_m
                while aux_ptr < len(aux_entries):
                    pk_a, rp_a, rs_a = aux_entries[aux_ptr]
                    if pk_a < pk_m:
                        _emit(pk_a, rp_a, rs_a)
                        aux_ptr += 1
                    else:
                        break

                _emit(pk_m, rp_m, rs_m)

        # Vuelca aux con PK mayor que todo el área principal
        while aux_ptr < len(aux_entries):
            pk_a, rp_a, rs_a = aux_entries[aux_ptr]
            _emit(pk_a, rp_a, rs_a)
            aux_ptr += 1

        # Escribe última página si quedó incompleta
        if out_slot > 0:
            self._write_page_header(out_page, out_slot)
            temp_pm.write_page(out_page_id, bytes(out_page))

        last_out_page = out_page_id

        # FASE 3 — Fix de next_pos (scan secuencial del temp)
        # El archivo ya es físicamente secuencial → next_pos es aritmético
        # I/O: P_out lecturas + P_out escrituras (ambas secuenciales)
        entry_global = 0  # Contador global de entradas ya escritas

        for page_id in range(1, last_out_page + 1):
            page        = bytearray(temp_pm.read_page(page_id))
            entry_count = self._read_page_header(page)

            for slot in range(entry_count):
                pk, rp, rs, _ = self._read_by_RID(page, slot)
                entry_global += 1
                is_last = (entry_global == total_valid)

                if is_last:
                    new_next = -1
                else:
                    # El siguiente entry está en el próximo slot (posición física directa)
                    next_entry_idx = entry_global          # 0-based del siguiente
                    next_page_id   = (next_entry_idx // self.entries_per_page) + 1
                    next_slot_id   = next_entry_idx %  self.entries_per_page
                    new_next = (next_page_id - 1) * temp_pm.PAGE_SIZE \
                            + 4 \
                            + next_slot_id * self.NODE_SIZE

                self._write_entry_on_page(page, slot, pk, rp, rs, new_next)

            temp_pm.write_page(page_id, bytes(page))

        # Reemplazamos el archivo y actualizamos metadata
        os.replace(temp_filename, self.filename)
        self.pm             = PageManager(self.filename)
        self.sparse_index   = new_sparse
        self.n_main         = total_valid
        self.k_aux          = 0
        self.last_main_page = last_out_page
        self.k_limit        = max(1, math.ceil(math.log2(max(1, total_valid))))
        self.first_logical_pos = 4  # Byte 4 de página 1 (primer slot tras header)
        self._persist_metadata()

    # ============ Métodos privados (helpers) ============
    def _decode_pk(self, raw_pk):
        if self.pk_is_str:
            return raw_pk.decode('utf-8', errors='ignore').rstrip('\x00')
        return raw_pk  # int y float ya son comparables
    
    def _tombstone_pk(self):
        if self.pk_is_str:
            return b'\x00' * self.pk_size
        fmt = self.pk_format.lstrip('<>=!')
        if fmt[-1] in ('f', 'd'):
            return -1.0
        return -1
    
    def _find_predecessor(self, pk: int) -> Tuple[int, int]:
        #Encuentra posiciones del predecesor y sucesor lógico de una PK (pred_pos, succ_pos)
        current_pos = self.first_logical_pos
        prev_pos = -1
        
        while current_pos != -1:
            page_id, slot_id = self._pos_to_page_slot(current_pos)
            page = self.pm.read_page(page_id)
            pk_entry, _, _, next_pos = self._read_by_RID(page, slot_id)
            
            if pk_entry >= pk:
                return (prev_pos, current_pos)
            
            prev_pos = current_pos
            current_pos = next_pos
        
        return (prev_pos, -1)
    
    def _find_first_greater_equal(self, pk: int) -> Optional[Tuple[int, int]]:
        if not self.sparse_index:
            return None
        
        # Búsqueda binaria: PRIMERA página con pk_primero >= pk
        left, right = 0, len(self.sparse_index) - 1
        candidate_idx = -1
        
        while left <= right:
            mid = (left + right) // 2
            mid_pk, _ = self.sparse_index[mid]
            if mid_pk >= pk:
                candidate_idx = mid
                right = mid - 1
            else:
                left = mid + 1
        
        if candidate_idx != -1:
            return (self.sparse_index[candidate_idx][1], 0)  # Directo
        
        # Aquí significa que pk > todas las páginas principales
        # Buscamos en auxiliar desde la última página del area principal
        last_page = self.sparse_index[-1][1]
        page = self.pm.read_page(last_page)
        entry_count = self._read_page_header(page)
        
        if entry_count > 0:
            _, _, _, last_next_pos = self._read_by_RID(page, entry_count - 1)
            if last_next_pos != -1:
                return self._follow_chain_ge(pk, last_next_pos)
        
        return None

    def _follow_chain_ge(self, pk: int, start_pos: int) -> Optional[Tuple[int, int]]:
        current_pos = start_pos
        visited = set()
        
        while current_pos != -1:
            if current_pos in visited:
                return None
            visited.add(current_pos)
            
            page_id, slot_id = self._pos_to_page_slot(current_pos)
            page = self.pm.read_page(page_id)
            pk_entry, _, _, next_pos = self._read_by_RID(page, slot_id)
            
            if pk_entry >= pk:
                return (page_id, slot_id)
            
            current_pos = next_pos
        
        return None
    
    def _find_entry_and_predecessor(self, pk: int) -> Tuple[int, int]:
        # Encuentra la entrada exacta de una PK y su predecesor; 
        # retorna (pred_pos, curr_pos)
        current_pos = self.first_logical_pos
        prev_pos = -1
        
        while current_pos != -1:
            page_id, slot_id = self._pos_to_page_slot(current_pos)
            page = self.pm.read_page(page_id)
            pk_entry, _, _, next_pos = self._read_by_RID(page, slot_id)
            
            if pk_entry == pk:
                return (prev_pos, current_pos)
            
            if pk_entry > pk:
                return (prev_pos, -1)
            
            prev_pos = current_pos
            current_pos = next_pos
        
        return (prev_pos, -1)
    
    def _follow_next_pos(self, next_pos: int) -> Optional[Tuple[int, int]]:
        if next_pos == -1:
            return None
        return self._pos_to_page_slot(next_pos)
    
    def _follow_chain(self, pk: int, start_pos: int) -> Optional[Tuple[int, int]]:
        # Sigue una cadena desde start_pos buscando una PK exacta
        current_pos = start_pos
        visited = set()
        
        while current_pos != -1:
            if current_pos in visited:
                return None
            visited.add(current_pos)
            
            page_id, slot_id = self._pos_to_page_slot(current_pos)
            page = self.pm.read_page(page_id)
            pk_entry, rid_p, rid_s, next_pos = self._read_by_RID(page, slot_id)
            
            if pk_entry == pk:
                return (rid_p, rid_s)
            
            current_pos = next_pos
        
        return None
    
    def _allocate_entry_in_aux(self) -> int:
        #Asigna un espacio para una nueva entrada en área auxiliar.
        total_entries = self.n_main + self.k_aux         # asigna al final del archivo
        pos = 4 + (total_entries * self.NODE_SIZE)  # 4 bytes de header por página, aprox.
        return pos # Posición absoluta (en bytes) donde escribir.
    
    def _pos_to_page_slot(self, pos: int) -> Tuple[int, int]:
        # posición absoluta (bytes) -> (page_id, slot_id dentro de la página)."""
        # Posición 0-4095: página 1, posición 4096-8191: página 2, etc.
        page_id = (pos // self.pm.PAGE_SIZE) + 1
        slot_offset = pos % self.pm.PAGE_SIZE
        slot_id = (slot_offset - 4) // self.NODE_SIZE  # -4 por el header
        return (page_id, slot_id)
    
    def _write_entry_at_pos(self, pos: int, pk: int, rid_p: int, rid_s: int, next_pos: int) -> None:
        #Escribe una entrada en una posición absoluta
        page_id, slot_id = self._pos_to_page_slot(pos)
        page = bytearray(self.pm.read_page(page_id))
        self._write_entry_on_page(page, slot_id, pk, rid_p, rid_s, next_pos)
        self.pm.write_page(page_id, bytes(page))
    
    def _read_by_RID(self, page: bytes, slot_idx: int) -> Tuple[int, int, int, int]:
        offset = 4 + (slot_idx * self.NODE_SIZE)
        raw = struct.unpack(self.NODE_FORMAT, page[offset:offset + self.NODE_SIZE])
        pk = self._decode_pk(raw[0])
        return pk, raw[1], raw[2], raw[3]  # (pk, rid_page, rid_slot, next_pos)
    
    def _write_entry_on_page(self, page: bytearray, slot_idx: int, pk: int, rid_p: int, rid_s: int, next_pos: int) -> None:
        offset = 4 + (slot_idx * self.NODE_SIZE)
        entry_bytes = struct.pack(self.NODE_FORMAT, pk, rid_p, rid_s, next_pos)
        page[offset:offset + self.NODE_SIZE] = entry_bytes
    
    def _read_page_header(self, page: bytes) -> int:
        return struct.unpack('<H', page[:2])[0]
    
    def _write_page_header(self, page: bytearray, entry_count: int) -> None:
        page[:2] = struct.pack('<H', entry_count)
