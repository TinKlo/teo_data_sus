

a_list = [2, 3,7,None]

print(f"This is a List:{a_list} ")

gen =range(0,10)
list(gen)

print(f" list(gen) - Listas geradoras: {list(gen)}")


# Adicionando e removendo elementos

b_list = ['fo', 'peekaboo', 'baz', 'astresobrons']

print(b_list)

b_list.append('dwarf')

print(b_list)

b_list.insert(1, 'red')

print(b_list)

# ? collections_deque

b_list.pop(2)

print(b_list)

# Concatenando e combinando listas

#Ordenação
a = [1,5,7,5,4,2,3,0]
print(a)
a.sort()
print(a)

print(b_list)
b_list.sort(key=len)
print(b_list)

print("ended pag 84")