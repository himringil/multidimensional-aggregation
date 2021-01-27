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
        return True

    def agg(old_value, new_value):
        if old_value is None:
            return new_value
        return old_value + new_value

class AggSum(AggFunction):
    
    def check(params):
        if not type(params) == list:
            return False
        for param in params:
            if not type(param) == list:
                return False
            if not type(param[0]) == str:
                return False
        return True

    def agg(old_value, new_value):
        if old_value is None:
            return new_value
        return old_value + new_value

class AggMin(AggFunction):
    
    def check(params):
        if not type(params) == list:
            return False
        for param in params:
            if not type(param) == list:
                return False
            if not type(param[0]) == str:
                return False
        return True

    def agg(old_value, new_value):
        if old_value is None:
            return new_value
        return min(old_value, new_value)

class AggMax(AggFunction):

    def check(params):
        if not type(params) == list:
            return False
        for param in params:
            if not type(param) == list:
                return False
            if not type(param[0]) == str:
                return False
        return True

    def agg(old_value, new_value):
        if old_value is None:
            return new_value
        return max(old_value, new_value)
