from parser.scanner import Scanner

def run_test():

    sql_input = 'CREATE TABLE usuarios (id INT, nombre VARCHAR(10)) PRIMARY KEY (id);'
    
    print(f"--- Analizando Consulta ---\n{sql_input}\n")
    
    try:
        lexer = Scanner(sql_input)
        
        tokens = lexer.tokenize()
        
        for i, token in enumerate(tokens):
            print(f"Token {i}: {token}")
            
    except SyntaxError as e:
        print(f"Opps! Algo salió mal: {e}")

if __name__ == "__main__":
    run_test()