import json
from matrix_utils import transpose_matrix

def lambda_handler(event, context):
    matrix = event.get('matrix2')
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

event = {'matrix1':[[1, 2, 3],[4, 5, 6],[7, 8, 9]],
         'matrix2':[[11, 12, 13],[14, 15, 16],[17, 18, 19]],
         'matrix3':[[21, 22],[23, 24],[25, 26]],
         'matrix4' : []}
lambda_handler(event,'b')