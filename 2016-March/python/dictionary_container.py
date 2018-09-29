#dictionary container creation and access
d = {'cat': 'cute', 'dog': 'furry'}  
d['cat']      
'cat' in d 
d['fish'] = 'wet' 
d['fish']     
d['monkey'] 
d.get('monkey', 'N/A')  
d.get('fish', 'N/A')   
del d['fish'] 
d.get('fish', 'N/A')

#loops
d = {'person': 2, 'cat': 4, 'spider': 8}
for animal in d:
    legs = d[animal]
    print '%s has %d legs' % (animal, legs)

for animal, legs in d.iteritems():
    print '%s has %d legs' % (animal, legs)

#dictionary comprehensions
nums = [0, 1, 2, 3, 4]
even_num_to_square = {x: x ** 2 for x in nums if x % 2 == 0}



