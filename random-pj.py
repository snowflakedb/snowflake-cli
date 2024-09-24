# A standard factory

import factory


class User:
    def __init__(self, language):

        self.language = language


class Company:
    def __init__(self, owner, country):
        self.owner = owner
        self.country = country


class Country:
    def __init__(self, language):
        self.language = language


# class UserFactory(factory.Factory):
#     class Meta:
#         model = User

#     # Various fields
#     first_name = 'John'
#     last_name = factory.Sequence(lambda n: 'D%se' % ('o' * n))  # De, Doe, Dooe, Doooe, ...
#     email = factory.LazyAttribute(lambda o: '%s.%s@example.org' % (o.first_name.lower(), o.last_name.lower()))

#     # Faker generated at runtime if we use factory.Faker("file_name")
#     @classmethod
#     def _create(cls, model_class, *args, **kwargs):
#         obj = cls._build(model_class, *args, **kwargs)
#         FAKE = factory.faker.faker.Faker()
#         fn = FAKE.file_name(extension="json")
#         # fn = factory.Faker("file_name")
#         # filename = factory.Faker("file_name", extension="json")
#         with open(fn, "w",encoding='utf-8') as file:
#              json_obj = json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=4)
#              json.dump(json_obj, file, ensure_ascii=False)
#         return obj
# # A factory for an object with a 'User' field
# class CompanyFactory(factory.Factory):
#     class Meta:
#         model = Company

#     name = factory.Sequence(lambda n: 'FactoryBoyz' + 'z' * n)
#     # Let's use our UserFactory to create that user, and override its first name.
#     owner = factory.SubFactory(UserFactory, first_name='Jack')

# # c = CompanyFactory()
# # print(c.owner.first_name)  # Jack
# # print(c.owner.email)

# # c2 = CompanyFactory(owner__first_name='Jill')
# # print(c2.owner.first_name)  # Jill
# # print(c2.owner.email)

# # c3 = CompanyFactory(owner__last_name='Dill')
# # print(c3.owner.first_name)
# # print(c3.owner.last_name)
# # print(c3.owner.email)

# class WithLazy:
#     def __init__(self, first_name, last_name, email):
#         self.first_name = first_name
#         self.last_name = last_name
#         self.email = email

# class FactoryWithLazy(factory.Factory):

#     @classmethod
#     def _build(cls, model_class, *args, **kwargs):
#         return json.dumps(**kwargs)

#     @classmethod
#     def _create(cls, model_class, *args, **kwargs):
#         if 'first_name' in kwargs:
#             kwargs['first_name'] = 'John'
#         return cls._build(model_class, *args, **kwargs)

#     first_name = 'John'
#     last_name = 'bee'
#     # email = factory.LazyAttribute(lambda o: '{}.{}@example.com'.format(o.first_name, o.last_name))

# fac = FactoryWithLazy()
# print(fac.email)


class UserFactory(factory.DictFactory):

    language = "en"


class CountryFactory(factory.DictFactory):

    language = "en"


# class CompanyFactory(factory.Factory):
#     class Meta:
#         model = Company

#     country = factory.SubFactory(CountryFactory)
#     owner = factory.SubFactory(UserFactory, language=factory.SelfAttribute('..country.language'))


class CompanyFactory(factory.Factory):
    class Meta:
        model = Company

    country = factory.SubFactory(CountryFactory)
    owner = factory.SubFactory(
        UserFactory,
        language=factory.LazyAttribute(
            lambda user: user.factory_parent.country["language"]
        ),
    )


CompanyFactory()
