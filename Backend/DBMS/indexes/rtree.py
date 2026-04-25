from typing import TypeAlias, Annotated, Literal
from dataclasses import dataclass
import numpy as np
import struct 

area: TypeAlias = list[list[float], list[float]]
mbrs: TypeAlias = list[area]
childs: TypeAlias = list[int]
rid: TypeAlias = list[int, str]

@dataclass 
class Header:
    N_RECORDS: int = 0
    ROOT_PTR: int = -1
    MIN_NODES: int = 2
    #MAX_NODES: int = MIN_NODES * 2

#TODO: Definir la longitud del ID con el equipo
@dataclass
class Node:

    # NonLeafNode: rectangulo ((x1,y1), (x1,y2))
    MBRS: mbrs # m <= size <= M elementos
    CHILDS: childs # m <= size <= M elementos
    N_KEYS: int = 0
    PAGE: int = -1

    # LeafNode: punto ((x1,y1), (x1,y1))
    RECORD_ID: str = ""
    LEAF_NODE: bool = True

    def pack(self, format: str) -> bytes:
      flat_mbrs   = [coord for mbr in self.MBRS for point in mbr for coord in point]
      rec_encoded = self.RECORD_ID.encode('utf-8').ljust(20, b'\x00')[:20]
      return struct.pack(format, *flat_mbrs, *self.CHILDS, self.N_KEYS, self.PAGE, rec_encoded, self.LEAF_NODE)

    @classmethod
    def unpack(cls, format: str, data: bytes, max_nodes: int) -> "Node":
        flat = struct.unpack(format, data)
        mbrs = [[[flat[i*4], flat[i*4+1]], [flat[i*4+2], flat[i*4+3]]] for i in range(max_nodes)]
        childs = list(flat[max_nodes*4 : max_nodes*5])
        n_keys, page = flat[max_nodes*5], flat[max_nodes*5+1]
        rec_id = flat[max_nodes*5+2].decode('utf-8').rstrip('\x00')
        is_leaf = flat[max_nodes*5+3]
        return cls(mbrs, childs, n_keys, page, rec_id, is_leaf)

class Rtree:
    #FILENAME: str

    #HEADER: Header
    HEADER_FORMAT = "3i"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    #NODE_FORMAT: str
    #NODE_SIZE: int

    #NOTE: Metodos privados
    def __read_node(self, offset: int) -> Node:
        with open(self.FILENAME, "rb+") as file:
            file.seek(offset)
            return Node.unpack(self.NODE_FORMAT, file.read(self.NODE_SIZE), self.HEADER.MIN_NODES * 2)

    def __write_node(self, offset: int, node: Node):
        with open(self.FILENAME, "rb+") as file:
            file.seek(offset)
            file.write(node.pack(self.NODE_FORMAT))

    def __write_header(self):
        with open (self.FILENAME, "rb+") as file:
            file.seek(0)
            file.write(struct.pack(
                self.HEADER_FORMAT,
                self.HEADER.N_RECORDS,
                self.HEADER.ROOT_PTR,
                self.HEADER.MIN_NODES
            ))

    def __choose_leaf(self, new_area: area):
        if (self.HEADER.ROOT_PTR == -1):
            self.HEADER.ROOT_PTR = self.HEADER_SIZE
            self.HEADER.N_RECORDS += 1
            max_nodes = self.HEADER.MIN_NODES * 2
            #NOTE: Limitado a 2D
            new_mbrs = [[[0, 0], [0, 0]] for _ in range(max_nodes)]
            new_childs = [-1 for _ in range(max_nodes)]
            root = Node(new_mbrs, new_childs)
            return (self.HEADER.ROOT_PTR, root)
        else:
            curr_node = self.__read_node(self.HEADER.ROOT_PTR)
            if (curr_node.LEAF_NODE == True): 
                return (self.HEADER.ROOT_PTR, curr_node)
            else:
                while (curr_node.LEAF_NODE != True):
                    min_expansion = 2e30
                    child_idx = -1
                    child_node = curr_node
                    child_ptr = -1
                    #NOTE: limitado a 2D
                    for mbr in curr_node.MBRS[:curr_node.N_KEYS]:
                        child_idx += 1
                        x1, y1 = mbr[0]
                        x2, y2 = mbr[1]
                        a1, b1 = new_area[0]
                        a2, b2 = new_area[1]
                        nx1 = min(x1, a1)
                        ny1 = min(y1, b1)
                        nx2 = max(x2, a2)
                        ny2 = max(y2, b2)
                        area_original  = abs(x2 - x1) * abs(y2 - y1)
                        area_expandida = abs(nx2 - nx1) * abs(ny2 - ny1)
                        new_expansion = area_expandida - area_original
                        if (new_expansion < min_expansion):
                            min_expansion = new_expansion
                            child_ptr = curr_node.CHILD[child_idx]
                            child_node = self.__read_node(child_ptr)
                    curr_node = child_node
                return (child_ptr, curr_node)
            
    def __linear_split():
        pass

    #NOTE: Metodos publicos
    def __init__(self, filename: str = "RtreeFile.bin", minimum_nodes: int = 2):
        self.FILENAME = filename
        try:
            with open(self.FILENAME, "rb+") as file:
                header_data = file.read(self.HEADER_SIZE)
                header_values = struct.unpack(self.HEADER_FORMAT, header_data)
                self.HEADER = Header(*header_values)
        except FileNotFoundError:
            self.HEADER = Header(MIN_NODES=minimum_nodes)
            with open(self.FILENAME, "wb") as file:
                file.write(struct.pack(
                    self.HEADER_FORMAT, 
                    self.HEADER.N_RECORDS, 
                    self.HEADER.ROOT_PTR,
                    self.HEADER.MIN_NODES
                ))
        finally:
            max_nodes = self.HEADER.MIN_NODES * 2
            #NOTE: Limitado a 2D
            self.NODE_FORMAT = f"{(max_nodes*'4f') + (max_nodes*'i')}2i20s?"
            self.NODE_SIZE = struct.calcsize(self.NODE_FORMAT)

    def insert(self, new_area: area, new_rid: rid):
        leaf_ptr, leaf_node = self.__choose_leaf(new_area)
        print("Nodo hoja encontrado: ", leaf_node)
        if (leaf_node.N_KEYS < self.HEADER.MIN_NODES * 2):
          leaf_node.MBRS[leaf_node.N_KEYS] = new_area
          leaf_node.CHILDS[leaf_node.N_KEYS] = -1
          leaf_node.PAGE, leaf_node.RECORD_ID = new_rid
          leaf_node.N_KEYS += 1
          self.__write_node(leaf_ptr, leaf_node)
        else:
            # me faltan:
          #split
          #Adjusttree
          pass
        self.__write_header()
        pass

    def remove():
        pass

    def rangeSearch(point: list[float, float], radious: int):
        pass

    def knn(point: list[float, float], k: int):
        pass

    def visualize():
        pass

# latitud = 1, longitud = 8
# point = np.array([[1,8], [1,8]])
# print(point)

if __name__ == "__main__":
    rtree = Rtree()

    point = np.array([[1,8], [1,8]])
    print(point[0])
    rtree.insert(point, [1, "sdad"])