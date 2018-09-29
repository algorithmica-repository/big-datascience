#tuple container creation and access
animals1 = ('cat', 'dog')
animals1[1]
animals1[2] = 'fish'

d = {(0,1):'cat', (1,2):'dog'}  
t = (0,1)       
print type(t)   
print d[t]    
print d[(1, 2)]

#loops
animals1 = ('cat', 'dog')

for animal in animals:
    print animal




