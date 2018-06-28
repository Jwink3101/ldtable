#!/usr/bin/env python
import pytest

import ldtable
_emptyList = ldtable._emptyList
ldtable=ldtable.ldtable

import sys

def test_list_val():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':['guitar','strings']},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':['bass','strings']},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':['guitar','strings']},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    DB = ldtable(items)    
    
    assert DB.count(role='strings') == 3
    assert DB.count({'role':'strings'},{'role':'bass'}) == 1
    assert DB.count({'role':'strings'},role='bass') == 1
    assert DB.count(role=['strings','bass']) == 1
    assert DB.count( (DB.Q.role==['strings','bass'])  ) == 1
    assert DB.count( (DB.Q.role==['strings','bass']) & (DB.Q.first=='Paul') ) == 1
    assert DB.count( (DB.Q.role=='strings') & (DB.Q.role=='bass') ) == 1
    
    return DB

def test_excluded_attributes():
    def _test():
        assert 'i**2' not in DB.attributes,'Should not have been added'
        with pytest.raises(KeyError):
            DB.query_one({'i**2':4})
    
    # Create with an excluded one
    DB = ldtable(exclude_attributes=['i**2'])

    # Try to add with an exclusion
    DB.add({'i':i,'i//2':i//2,'i**2':i**2} for i in range(10))
    _test()
    
    # Reindex
    DB.reindex() # Should do it silently since it is implicit
    _test()
    with pytest.raises(ValueError):
        DB.reindex('i**2')
    _test()
    
    # Try to add it explicitly
    with pytest.raises(ValueError):
        DB.add_attribute('i**2')
    _test()

    # Try to add a new item
    i = 10
    DB.add({'i':i,'i//2':i//2,'i**2':i**2})
    _test()
    
    # Update that value on an item
    DB.update({'i**2':21.1},DB.Q.i==1)
    _test()
    assert DB.query_one(i=1)['i**2'] == 21.1,'Should still have updated value'

    # This tests an internal method that is a last guard against not 
    # allowed attributes
    with pytest.raises(ValueError):
        DB._append('i**2',0,0)

    return DB
    
def test_adding_objects():
    DB = ldtable(indexObjects=True)
    
    class OBJ(object):
        def __init__(self,**KW):
            for key,val in KW.items():
                setattr(self,key,val) 
    
    A = OBJ(a=1,b=2)
    DB.add(A)
    
    DB.add({'a':2,'b':3,'c':4}) # Note that this also tests add_attribute
    
    # Check that the first item is still A and not a dict
    assert next(DB.items()) == A
    assert next(DB.items()) is A
    assert isinstance(A,OBJ)
    assert not isinstance(A,dict)
    
    # Check that 'c' was added to A
    assert hasattr(A,'c')
    
    # Test queries on it
    assert A    is next(DB(a=1)) \
                is next(DB.query(a=1)) \
                is next(DB.query({'a':1}) ) \
                is next(DB({'a':1}) )\
                is DB[{'a':1}] \
                is next(DB.query(DB.Q.a==1)) \
                is next(DB(A))
    
    # Advanced Queries 
    # inequality queries
    Q = DB.Q
    assert DB.query_one( Q.a <  2 ) is A
    assert DB.query_one( Q.a <= 1 ) is A
    assert DB.query_one( Q.b >  2 ) is not A
    assert DB.query_one( Q.b >= 3 ) is not A
    assert DB.query_one( Q.a != 2 ) is A
    assert DB.query_one( ~(Q.a == 2) ) is A
    assert DB.query_one( (Q.b > 2) & (Q.a < 2)) is None
    
    # Filters: Same as Q.a == 1 but uses filter mechanisms (note: O(N))
    filt = lambda item: True if item['a'] == 1 else False 
    assert DB.query_one(Q.filter(filt)) is A
    assert DB.query_one(Q._filter(filt)) is A
    
    
    # Change a value and reindex, then test it
    next(DB(A)).c = 40
    DB.reindex()
    assert next(DB(c=40)) is A
    
    # Change the value with update using A as the query
    DB.update({'c':5},A)    # Update with dict
    assert A.c == 5         # Update should propagate through
    
    DB.update(OBJ(b=50),A)  # Update with a new object
    assert A.b == 50   
    
    # Add another item that matches similar items as A
    DB.add({'a':1,'b':50,'c':5,'d':200})

    # Create a new obj that had a,b,c the same and show that we get both this
    # and the added item
    B = OBJ(a=1,b=50,c=5)
    assert len(list(DB.query(B))) == 2 # Both are there
    assert any(item is A for item in DB.query(B)) # One of them is A
    
    # Quick isin check
    assert DB.isin(A)
    assert A in DB
    assert any(item is A for item in DB.items())
    
    # Finally remove it. Use a query that will only match A. 
    L0 = len(DB)
    DB.remove(a=1,d=None)
    
    assert len(DB) == (L0-1)
    assert A not in DB
    assert not any(item is A for item in DB.items())
    
    # Add a new item with a list
    C = OBJ(a=[9,10])
    DB.add(C)
    
    Q = DB.Q
    assert next(DB(Q.a == 9)) is C
    assert next(DB( (Q.a == 9) & (Q.a <10) )) is C
    with pytest.raises(StopIteration):
        assert next(DB( (Q.a == 8) & (Q.a <10) )) == []
    
    # add back in A then show what happens when you do not index objects
    DB.add(A)
    assert next(DB.query(A)) is A
    
    DB.indexObjects=False
    
    with pytest.raises(ValueError):
        DB.query_one(A)     # Should fail
    
    with pytest.raises(AttributeError):
        DB.add(OBJ(a=2))
        
    # it IS still there through. Just trapped
    assert any(item is A for item in DB.items())

    # Other queries work
    assert DB.query_one(a=2) == {'a': 2, 'b': 3, 'c': 4, 'd': None}

def test_empty():
    """Operations on empty DB. Mostly testing for errors"""
    # Empty with attributes
    DB = ldtable()

    # Query    
    assert not {'a':'i'} in DB # should also not cause an error
    assert list(DB.query(DB.Q.a=='i')) == []

    # Add attribute
    DB.add_attribute('bb',[])
    assert 'bb' in DB.attributes
    
    # Add something
    DB.add({'a':1,'bb':2,'x':3})
    
    assert 'a' in DB.attributes
    assert 'x' in DB.attributes
    
    # Just test the _empty object
    empty = _emptyList()
    assert empty == []
    assert not empty == [1]
    
def test_adv_queries():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    
    # Remove an item so we cal also test with removed items not showing
    DB.remove(first='Paul')
    assert len(DB) == 4
    
    DB.alwaysReturnList = True
    
    Q = DB.Q
    
    assert len(list( DB.query(Q._index==0))) == 1
    assert len(list( DB.query(Q._index==1))) == 0
    
    assert len(list( DB.query(Q.born <= 1940))) == 3
    assert len(list( DB.query( (Q._index == 2) & (Q.born <= 1940) ))) == 0
    assert len(list( DB.query( (Q.born <= 1940)& (Q._index == 2)  ))) == 0
    assert len(list( DB.query( (Q.first == 'George') & (Q._index == 1)  ))) == 0
    
    assert len(list( DB.query(Q.born < 1940))) == 1
    assert len(list( DB.query( (Q._index == 2) & (Q.born < 1940) ))) == 0
    assert len(list( DB.query( (Q.born < 1950)& (Q._index == 2)  ))) == 1
    
    assert len(list( DB.query(Q.born >= 1940))) == 3
    assert len(list( DB.query( (Q._index == 2) & (Q.born >= 1940) ))) == 1
    assert len(list( DB.query( (Q.born >= 1940) & (Q._index == 2) ))) == 1
    
    assert len(list( DB.query(Q.born > 1940))) == 1
    assert len(list( DB.query( (Q._index == 1) & (Q.born > 1940) ))) == 0
    assert len(list( DB.query( (Q.born > 1940)& (Q._index == 1)  ))) == 0

    assert len(list( DB.query( (Q.first == 'Ringo') | ~(Q.first == 'George')))) == 2
    assert len(list( DB.query( (Q.first == 'Ringo') | (Q.first != 'George')))) == 2

    # Try an incomplete query. Should just return nothing
    assert list(DB(Q.first)) == []

def test_items_iteritems():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    DB.alwaysReturnList = False
    
    for ii,item in enumerate(DB.items()):
        assert item == items[ii]
    
    for ii,item in enumerate(DB):
        assert item == items[ii]
    
    # Generators don't have lengths
    with pytest.raises(TypeError):
        len(DB.items())
        
    import types
    assert isinstance(DB.items(),types.GeneratorType)
    

def test_index():    
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    DB.alwaysReturnList = False
    
    Q = DB.Q
    for ii in range(len(DB)):
        assert DB[ii] == items[ii] # [ ] is yield one
        assert list(DB.items())[ii] == items[ii]
        assert DB.query_one(_index=ii) == items[ii]
        assert DB.query_one(Q._index==ii) == items[ii]
    
    assert list(DB( (Q._index==0) & (Q._index==1) ) ) == []  
    assert list(DB(Q._index==[0,1]) ) == []
    assert list(DB(_index=100) ) == []
    
def test_removal():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    
    assert DB[3] == items[3]
    
    # This also tests the `_index` term
    DB.remove(_index=3)

    with pytest.raises(ValueError): # it won't let you get this item
        DB[3]
    
    # Can get the third element since it only returns non deleted
    assert list(DB.items())[3] == items[4] 
    
    assert len(list(DB.query(_index=3))) == 0 # Nothing there
    assert len(list(DB.query(first='Ringo'))) == 0 # Nothing there
    
    assert len(list(DB.items())) == 4
    assert len(DB) == 4
    assert DB.N == 4
    
    with pytest.raises(ValueError): # No matches
        DB.remove(first='Peter')
    
    # Remove an empty list element
    DB.query_one(first='Paul')['role'] = []
    DB.reindex('role')
    assert DB.query_one(role=[])['first'] == 'Paul' # Test it
    DB.remove(role=[])
    assert DB.query_one(role=[]) is None
    
    # Try to delete after a change without reindex
    DB.query_one(born=1940)['last'] = 'no last name'
    #... do not reindex
    with pytest.raises(ValueError): # No matches
        DB.remove(born=1940)

def test_reindex_update():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    
    Q = DB.Q
    
    assert DB.query_one(born=1926) == items[4]
    
    # Change it
    DB.query_one(born=1926)['born'] = 1927
    
    # This should fail but it doesn't since we didn't reindex
    assert DB.query_one(born=1926) == items[4]
    
    # This shouldn't fail but it does since we didn't reindex (add a `not` to pass)
    assert not DB.query_one(born=1927) == items[4]
    
    # This should also fail since the Qobj's aren't updated
    assert not DB.query_one(Q.born==1927) == items[4]
    assert DB.query_one(Q.born==1926) == items[4]
    
    # To show it *was* updated
    assert DB.query_one(last='Martin')['born'] == 1927
    
    # Reindex
    DB.reindex('born')
    Q = DB.Q # Reinstantiate Q
    
    # now it all flips
    assert not DB.query_one(born=1926) == items[4]
    assert DB.query_one(born=1927) == items[4]
    assert DB.query_one(Q.born==1927) == items[4]
    assert not DB.query_one(Q.born==1926) == items[4]
    
    assert DB.query_one(last='Martin')['born'] == 1927
    
    
    # Now, switch it back with update but do not reindex
    DB.update({'born':1926},DB.Q.born==1927)
    assert DB.query_one(born=1926) == items[4]
    assert not DB.query_one(born=1927) == items[4]
    
    # other update style
    DB.update({'born':1926},born=1926) # This doesn't change anything in reality
    
    # Add a `not` to this one
    assert not DB.query_one(last='Martin')['born'] == 1927
    
    # Multiple
    DB.update({'first':'Ringo'},{'first':'George'})
    assert len(list(DB.query(first='Ringo'))) == 3
    
    ## Errors
    
    # Test bad syntax
    with pytest.raises(ValueError):
        DB.update({'born':1940},{'first':'Ringo'},{'role':'drums'})     # Should fail
    
    # Test bad syntax 2
    with pytest.raises(ValueError):
        DB.update(DB.Q.born==1940,{'first':'Ringo'})
    
    # Test no results
    with pytest.raises(ValueError):
        DB.update({'born':1940},{'first':'ringo'})
    
    with pytest.raises(ValueError):
        DB.update({'born':1940},[{'first':'ringo'}])
    
    
    
def test_all_query_methods():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},      # 0
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},     # 1
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},   # 2
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},        # 3
        {'first':'George','last':'Martin','born':1926,'role':'producer'}    # 4
    ]
    
    DB = ldtable(items)
    
    Q = DB.Q
    
    ## Single Queries 
    # * Dict vs attrib=val, 
    assert DB.query_one(first='John') == items[0]
    assert DB.query_one({'first':'John'}) == items[0]
    assert DB.query_one(Q.first=='John') == items[0]
    
    # * __call__ maps to query()
    assert next( DB(first='John') ) == items[0]
    assert next( DB(Q.first=='John') ) == items[0]
    assert next( DB({'first':'John'}) ) == items[0]
    
    # * __getitem__ of non integer is also queryone
    assert DB[{'first':'John'}] == items[0]    
    for ii in range(5):
        assert DB[ii] == items[ii]
    
    with pytest.raises(ValueError):
        assert DB['Paul']['last'] == 'McCartney' 
    
    
    ## Multi Queries
    assert DB.query_one( first='George',last='Harrison') == items[2]
    assert DB.query_one( Q.first=='George',last='Harrison') == items[2]
    assert DB.query_one( (Q.first=='George') & (Q.last=='Harrison') ) == items[2]
    assert DB.query_one( {'first':'George'}, last='Harrison' ) == items[2]
    assert DB.query_one( {'first':'George','last':'Harrison'}) == items[2]
    
    ## in queries
    assert {'first':'George','last':'Harrison'} in DB
    assert (Q.first=='George') in DB
    assert DB.isin(first='John')
    assert {'first':'George','last':'Starr'} not in DB
    with pytest.raises(ValueError):
        assert 'George' in DB
    
    
    ## Three queries
    assert DB.query_one( (Q.first=='George') & (Q.born<2000),last='Harrison' ) == items[2]
    
    
        

def test_add_attribute():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},
        {'first':'George','last':'Martin','born':1926,'role':'producer','extra':'test'} # Additional
    ]
    
    DB = ldtable(items,attributes=['first','last','born','role'])
    DB.alwaysReturnList = True
    
    # Should fail since 'extra' is not an attribute
    with pytest.raises(KeyError):
        DB.query_one(extra='test')
    
    # This should fail since not all items have 'extra'
    with pytest.raises(KeyError):
        DB.add_attribute('extra')
        
    DB.add_attribute('extra','added')
    assert len( list( DB.query(extra='test') )) == 1
    assert len( list( DB.query(extra='added') )) == 4

    # Add an empty attribute of a list-type so I can add to it later
    DB.add_attribute('bands',[])
    assert 'bands' in DB.attributes

def test_Qobj_expiry():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},
        {'first':'George','last':'Martin','born':1926,'role':'producer','extra':'test'}
    ]    
    
    DB = ldtable(items)
    DB.alwaysReturnList = False
    
    Q = DB.Qobj # or DB.Q
    assert DB.query_one( Q.first=='John' ) == items[0]
    
    DB.reindex()    
    with pytest.raises(ValueError): # Should be out of date
        assert DB.query( Q.first=='John' ) == items[0]
    
    
    DB.reindex()
    Q = DB.Qobj
    assert DB.query_one( Q.first=='John' ) == items[0]
    
    # This should pass since we create the Qobj in line at the time of query
    DB.reindex()
    assert DB.query_one( DB.Q.first=='John' ) == items[0]
    
    

def test_init_empty_v_full():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},
        {'first':'George','last':'Martin','born':1926,'role':'producer','extra':'test'}
    ]
    
    DB1 = ldtable()
    for item in items:
        DB1.add(item)
    
    assert len(DB1) == 5
    assert len(list(DB1.query(extra=None))) == 4
    
    DB2 = ldtable(items)
    
    assert len(DB1) == 5
    assert len(list(DB1.query(extra=None))) == 4
    
    DB3 = ldtable()
    DB3.add(items)
    
    assert len(DB3) == 5
    assert len(list(DB3.query(extra=None))) == 4
    
    
    DB11 = ldtable(attributes=['first','last','born','role'])
    
    for item in items:
        DB11.add(item)
    assert len(DB11) == 5
    
    with pytest.raises(KeyError):
        assert len(list(DB11.query(extra=None))) == 4
    

    DB21 = ldtable(items=items,attributes=['first','last','born','role'])
    
    assert len(DB21) == 5
    
    with pytest.raises(KeyError):    
        assert len(list(DB21.query(extra=None))) == 4
    
    
def test_queries():
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},
        {'first':'George','last':'Martin','born':1926,'role':'producer'}
    ]
    
    DB = ldtable(items)
    
    DB.alwaysReturnList = True
    
    # Single item
    result = DB.query(first='George')
    assert items[2] in result
    assert items[4] in result
    
    # Multiple query
    result = DB.query(first='George',role='guitar')
    assert items[2] in result
    assert items[4] not in result
    
    # advanced query 1
    Q = DB.Q
    
    result = DB.query(Q.born <= 1940)
    for ii in [0,3,4]:
        assert items[ii] in result
    
    # advanced query 2
    Q = DB.Q
    result = list( DB.query( (Q.born <= 1940) & (Q.role == 'producer')) )
    assert items[0] not in result
    assert items[3] not in result
    assert items[4] in result
    
    # Test no results
    assert list(DB.query(role='computer scientist')) == []

    # Error on queries
    with pytest.raises(ValueError):
        DB.query_one([{'first':'John'}]) # Cannot query a list

def test_filters():
    """ Test things relating to filters. Also tests other parts of the code"""
    items = [
        {'first':'John', 'last':'Lennon','born':1940,'role':'guitar'},
        {'first':'Paul', 'last':'McCartney','born':1942,'role':'bass'},
        {'first':'George','last':'Harrison','born':1943,'role':'guitar'},
        {'first':'Ringo','last':'Starr','born':1940,'role':'drums'},
        {'first':'George','last':'Martin','born':1926,'role':'producer'}
    ]
    
    DB = ldtable(items)
    
    DB.alwaysReturnList = True
    
    # This is of course the same as an equality
    Q = DB.Q
    filt = lambda item: True if item['first'] == 'George' and item['born'] < 1940 else False
    assert items[4] in DB.query(Q.filter(filt))    
    
    ## When an attribute is named 'filter'
    # Build a new attribute (and test that)
    import itertools
    _AB = itertools.cycle(['A','B'])
    
    if hasattr(_AB,'next'):
        AB = lambda: _AB.next()
    else:
        AB = lambda: _AB.__next__()
    
    DB.add_attribute('filter',AB)
    
    # Check that it worked
    for item in DB.items():
        assert item['filter'] in ['A','B']
    
    Q = DB.Q
    filt = lambda item: True if item['filter'] == 'A' else False
    
    # This should fail
    with pytest.raises(TypeError):
        assert  len(DB.query(Q.filter(filt))) == 3  
   
    assert  len(list(DB.query(Q._filter(filt)))) == 3  
    
    # Test with a removal
    DB = ldtable(items)
    DB.remove(last='Martin')
    Q = DB.Q
    filt = lambda item: True if item['first'] == 'George' and item['born'] < 1940 else False
    assert items[4] not in DB(Q._filter(filt))    
    
    

def test_default_attribs_callable():
    """ Test adding callable attributes """
    
    class call_meOBJ(object):
        def __init__(self):
            self.i = 0
        def __call__(self):
            self.i += 1
            return self.i - 1
    
    # A callable object
    call_me = call_meOBJ()
    
    DB = ldtable(default_attribute=call_me)
    DB.alwaysReturnList = False
    
    DB.add({'a':1,'b':2})
    DB.add({'a':2,'b':4})
    
    # Now add something new
    DB.add({'a':4,'b':8,'c':16})
    
    # Make sure the other two items have a 'c' that is either 0 or 1
    assert DB.query_one(a=1)['c'] == 0
    assert DB.query_one(a=2)['c'] == 1
    
    # Now add yet another new item that *doesn't* have 'b'. Make sure this works!
    DB.add({'a':8,'c':32,'cc':64})
    
    # old ones
    assert DB.query_one(a=1)['cc'] == 2
    assert DB.query_one(a=2)['cc'] == 3
    assert DB.query_one(a=4)['cc'] == 4
    
    # new one with 'b'
    assert DB.query_one(a=8)['b'] == 5

def test_default_attribs_not_callable():
    """ Test adding non-callable attributes """
    
    
    default = 5
    
    DB = ldtable(default_attribute=default)
    DB.alwaysReturnList = False
    
    DB.add({'a':1,'b':2})
    DB.add({'a':2,'b':4})
    
    # Now add something new
    DB.add({'a':4,'b':8,'c':16})
    
    # Make sure the other two items have a 'c' that is either 0 or 1
    assert DB.query_one(a=1)['c'] == 5
    assert DB.query_one(a=2)['c'] == 5
    
    # Now add yet another new item that *doesn't* have 'b'. Make sure this works!
    DB.add({'a':8,'c':32,'cc':64})
    
    # old ones
    assert DB.query_one(a=1)['cc'] == 5
    assert DB.query_one(a=2)['cc'] == 5
    assert DB.query_one(a=4)['cc'] == 5
    
    # new one with 'b'
    assert DB.query_one(a=8)['b'] == 5
    
    # Now, to make sure they are all the same object, no copying
    
    assert DB.query_one(a=1)['c'] is default
    assert DB.query_one(a=2)['c'] is default
    
    assert DB.query_one(a=1)['cc'] is default
    assert DB.query_one(a=2)['cc'] is default
    assert DB.query_one(a=4)['cc'] is default
    
    assert DB.query_one(a=8)['b'] is default



if __name__ == '__main__':
    test_removal()

