a = "Insert another string here: {}".format("Insert Me!")
print(a)

b = "Insert one: {x} Item two: {y}".format(x = "dog", y="cat")
print(b)

mylist = ['stringasasd', 1,2,3,23.2,True,'asdg', [1,2,3]] # we can have mixed type in the array
mylist[0] = 'NEW ITEM' # python arrays are mutable

mylist.append("comes in at the end of the list")

mylist1 = [1,2,3]
mylist2 = [4,5,6]

mylist1.extend(mylist2) # this adds all of the elements of list 2 to list 1

mylist1.pop() # if no index is given it pops the last element in the array and also removes it form the array in the process

matrix = [[1,2,3],[4,5,6],[7,8,9]]

matrix[0][0] # return 1

first_col = [row[0] for row in matrix] # gives [1,4,7]

# Dictionaries

my_stuff = {"key1":"value1", "key2":"value2", "key3":{'123':[1,2,'grabMe']}}

print(my_stuff["key1"]) # gives value1.
print(my_stuff['key3']['123'][2]) # gives grabMe

my_stuff['key1'] = 'monkey' # changes value1 to monkey. 

my_stuff['key4'] = 'newItem' # adds this new key-value pair to the dictionary

# Tuples
t = (1,2,3)
print(t[0]) # gives 1
# tuples are immutable sequences and can also keep mixed types

# Sets unique unordered elements.

x = set()
x.add(1)
x.add(2)

converted = set([1,2,3,3,3,3,4,5,6,6,6,7,8,9,9,9,9])
print(converted) # gives {1,2,3,5,6,7,8,9}

# Control Flow
if 1<2:
	print('yes!')
elif 3 == 3:
	print('elif ran')
else:
	print('no')

if n in [1,2,3,4]: # in this way we can check if n is in a some type of a range in a list. pretty cool.
	return True
return False

seq = [1,2,3,4,5,6]

for item in seq: # note 'item' is just a variable name.
	# code here
	print('hello')

d = {'sam':1, 'frank':2}
for item in d:
	print(item) # prints the keys
	print(d[k]) # prints values

# example of for tuple unpacking 
mypairs = [(1,2),(3,4),(5,6)]

for tup1,tup2 in mypairs:
	print(tup1)
	print(tup2)

# Rembember to use 'range() objects if one ever has a list with a predictable sequence to save memory.'

# Neat list comprehension trick

x = [1,2,3,4]

out = [num**2 for num in x] # gives a list [1,4,9,16]

# Functions

def my_func(param1 = 'default'):
	"""
	This is a DOCSTRING
	"""
	print('some random function')

my_func()

# Filter
mylist3 = [1,2,3,4,5,6]

def even_bool(num):
	return num%2 == 0

evens = filter(even_bool, mylist3)

print(list(evens)) # gives all the even numbers from list above, we need the list() function because filter does not make a list it is a generator

# Lambda Expression
evens = filter(lambda num:num%2==0, mylist3)
print(list(evens))
























