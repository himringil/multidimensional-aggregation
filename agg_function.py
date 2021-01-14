from abc import ABC, abstractmethod

class AggFunction(ABC):

    @staticmethod
    @abstractmethod
    def check(params):
        pass

    @staticmethod
    @abstractmethod
    def agg(old_value, new_value):
        pass

class AggCount(AggFunction):
    
    def check(params):
        pass

    def agg(old_value, new_value):
        return old_value + new_value

class AggSum(AggFunction):
    
    def check(params):
        pass

    def agg(old_value, new_value):
        return old_value + new_value

class AggMin(AggFunction):
    
    def check(params):
        pass

    def agg(old_value, new_value):
        return min(old_value, new_value)

class AggMax(AggFunction):

    def check(params):
        pass

    def agg(old_value, new_value):
        return max(old_value, new_value)
