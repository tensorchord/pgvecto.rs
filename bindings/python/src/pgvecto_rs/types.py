import collections

SparseVector = collections.namedtuple("SparseVector", "dims indexs values")


def print_sparse_vector(sparse_vector):
    dense = [0.0] * sparse_vector.dims
    for i, v in zip(sparse_vector.indexs, sparse_vector.values):
        dense[i] = v
    return str(dense)


# override __str__ method of SparseVector
SparseVector.__str__ = print_sparse_vector
