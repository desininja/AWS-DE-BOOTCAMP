def transpose_matrix(matrix):
    
    transposed_matrix = list(map(list, zip(*matrix)))
    #transposed_matrix = [[row[i] for row in matrix] for i in range(len(matrix[0]))]
    
    return transposed_matrix
