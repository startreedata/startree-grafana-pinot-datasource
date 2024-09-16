package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet_Add(t *testing.T) {
	s := NewSet[int](0)
	s.Add(1)
	s.Add(2)
	assert.ElementsMatch(t, []int{1, 2}, s.Values())
}

func TestSet_Remove(t *testing.T) {
	s := NewSet[int](0)
	s.Add(1)
	s.Add(2)
	s.Del(1)
	assert.ElementsMatch(t, []int{2}, s.Values())
}

func TestSet_Contains(t *testing.T) {
	s := NewSet[int](0)
	s.Add(1)
	s.Add(2)
	assert.True(t, s.Contains(1))
	assert.True(t, s.Contains(2))
}

func TestSet_Len(t *testing.T) {
	s := NewSet[int](0)
	s.Add(1)
	s.Add(2)
	assert.Equal(t, 2, s.Len())
}
