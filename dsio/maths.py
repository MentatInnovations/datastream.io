import numpy as np

def range(x):
    return(np.max(x) - np.min(x))




def convex_combination(a,b,weight):
    return (1-weight) * a + weight * b
assert(np.mean([3,7]) == convex_combination(3,7,0.5))
assert(np.mean([1,1,1,2]) == convex_combination(1,2,0.25))
