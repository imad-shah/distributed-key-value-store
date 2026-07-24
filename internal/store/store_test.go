package store

import (
	"sync"
	"testing"
)

const (
	missingKey = "there is no way this key exists in the map because i just made it up230-9dfj31290fn3109nf0138nfd0813bnfd0813bnf-8012bnr0-8`21br0821"
)

func TestGet(t *testing.T) {
	store := New()
	key := "1"
	value := "10"
	store.Set(key, value)
	if val, ok := store.Get(key); !ok || val != value {
		t.Errorf("Get(%q) = %q, %v; want %q, true", key, val, ok, value)
	}
}

func TestSet(t *testing.T) {
	store := New()
	key := "1"
	value := "10"
	store.Set(key, value)
	if get, _ := store.Get(key); get != value {
		t.Errorf("Get(%q) got %q, wanted %q", key, get, value)
	}
	newValue := "20"
	store.Set(key, newValue)
	if get, _ := store.Get(key); get != newValue {
		t.Errorf("Get(%q) got %q, wanted %q", key, get, newValue)
	}
}

func TestDelete(t *testing.T) {
	store := New()
	key := "1"
	value := "10"
	store.Set(key, value)
	if get, _ := store.Get(key); get != value {
		t.Errorf("Get(%q) got %v, wanted %v", key, get, value)
	}
	store.Delete(key)
	if _, ok := store.Get(key); ok {
		t.Errorf("Get(%q) after delete: ok = true, want false", key)
	}
}

func TestGetAndDeleteSameTime(t *testing.T) {
	store := New()
	var wg sync.WaitGroup
	wg.Add(2)

	store.Set("1", "10")

	go func() {
		defer wg.Done()
		for range 100 {
			store.Get("1")
		}
	}()

	go func() {
		defer wg.Done()
		for range 100 {
			store.Delete("1")
		}
	}()

	wg.Wait()
}

func TestGetKeyDoesntExist(t *testing.T) {
	store := New()
	if _, ok := store.Get(missingKey); ok {
		t.Errorf("Get(%q) was found in map when it doesn't exist", missingKey)
	}
}

func TestDeleteOnKeyDoesntExist(t *testing.T) {
	store := New()
	if ok := store.Delete(missingKey); ok {
		t.Errorf("Delete(%q) worked when key doesn't exist", missingKey)
	}

}
