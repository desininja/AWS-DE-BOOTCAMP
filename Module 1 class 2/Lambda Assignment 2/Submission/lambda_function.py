import json
from matrix_utils import transpose_matrix

def lambda_handler(event, context):
    matrix = event.get('matrix1')
    print("Original Matrix:")
    for row in matrix:
        print(row)
    
    transposed_matrix = transpose_matrix(matrix)
    print("Transposed Matrix: ")
    for row in transposed_matrix:
        print(row)
    
    matrix = event.get('matrix2')
    print("Original Matrix:")
    for row in matrix:
        print(row)
    
    transposed_matrix = transpose_matrix(matrix)
    print("Transposed Matrix: ")
    for row in transposed_matrix:
        print(row)
        
    matrix = event.get('matrix3')
    print("Original Matrix:")
    for row in matrix:
        print(row)
    
    transposed_matrix = transpose_matrix(matrix)
    print("Transposed Matrix: ")
    for row in transposed_matrix:
        print(row)
        
    matrix = event.get('matrix4')
    print("Original Matrix:")
    for row in matrix:
        print(row)
    
    transposed_matrix = transpose_matrix(matrix)
    print("Transposed Matrix: ")
    for row in transposed_matrix:
        print(row)
        
        
    return {
        'statusCode': 200,
        'body': json.dumps('Transposed worked successfully')
    }
