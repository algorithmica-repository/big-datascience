#set container creation and access
animals = set(['cat', 'dog'])
'cat' in animals   
'fish' in animals 
animals.add('fish')      
'fish' in animals 
len(animals)       
animals.add('cat') 
len(animals) 
animals.remove('cat')  
len(animals)    

#loops
animals = set(['cat', 'dog', 'fish'])

for animal in animals:
    print animal
for idx, animal in enumerate(animals):
    print '#%d: %s' % (idx + 1, animal)



