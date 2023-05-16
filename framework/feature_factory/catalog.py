from .feature import Feature


class CatalogBase:
    @classmethod
    def get_all_features(cls):
        """
        Returns a dict of features defined in a catalog class.
        Note that the variables include those inherited from the parent classes, and are 
        updated based on the reversed method resolution order.

        In the example below, while `class C` inherits props defined in `class A` and `class B`,
        props defined in `class C` will overwrite props defined in `class A`, which will in turn
        overwrite those defined in `class B`:
        >>> class A:
        >>>     pass
        >>> class B:
        >>>     pass
        >>> class C(A, B):
        >>>     pass
        """

        members = dict()
        for aclass in reversed(cls.__mro__):
            vars_dct = vars(aclass)
            for nm, variable in vars_dct.items():
                if not isinstance(variable, Feature): continue
                if not callable(getattr(aclass, nm)) and not nm.startswith("__"):
                    members[nm] = variable
                    variable.set_feature_name(nm)
        return members
