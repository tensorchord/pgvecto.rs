import collections

SparseVector = collections.namedtuple("SparseVector", "dims indices values")


def print_sparse_vector(sparse_vector):
    dense = [0.0] * sparse_vector.dims
    for i, v in zip(sparse_vector.indices, sparse_vector.values):
        dense[i] = v
    return str(dense)


# override __str__ method of SparseVector
SparseVector.__str__ = print_sparse_vector
