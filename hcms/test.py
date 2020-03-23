from uuid import uuid4
from session_loader import *


# def is_string_empty(str):
#     # if not (str and not str.isspace()):
#     if not (str and str.strip()):
#         # if not str:
#         return True
#     else:
#         return False


def testing():
    my_filter = {
        "process_status__c": "MOBILE FIELDS",
        "status__c": "IN PROGRESS",
    }
    for key, value in my_filter.items():
        print("CHECK {} {}".format(key, value))


def query_organization_source(session, query_limit, **kwargs):
    """-----------------------------------------------------------
    Description: Will query all of the records from the StageContact table and return contact list 
    Argument: db session
    -----------------------------------------------------------"""
    print("TEST query_organization_source")
    try:
        q = session.query(OrganizationSource)
        for key, value in kwargs.items():
            f = getattr(OrganizationSource, key)
            q = q.filter(f.in_(value))
        organization_sources = q.limit(query_limit)
        # TODO: CATCH ERRORS
    except expression as identifier:
        print("THERE WAS AN ERROR WHILE QUERYING ORG SOURCES")

    for org in organization_sources:
        print("CHECK ORG {} {}".format(org.client_id__c, org.is_active__c))
    return organization_sources


if __name__ == "__main__":
    session = loadSession()

    # query_organization_source(session, 100, client_id__c=["1111111111"])
    # testing()
    print("CHECK ID : {}".format(uuid4().hex[:18]))
    print("CHECK ID : {}".format(uuid4().hex[:18]))
    print("CHECK ID : {}".format(uuid4().hex[:18]))

    # str1 = ""
    # str2 = "   "
    # str3 = None

    # print(is_string_empty(str1))
    # print(is_string_empty(str2))
    # print(is_string_empty(str3))

    # organization_sources = (
    #     session.query(OrganizationSource)
    #     .filter(OrganizationSource.is_active__c == True)
    #     .limit(100)
    # )

#     Session.new - for objects, which will be added to database.
# Session.dirty - for objects, which will be updated.
# Session.deleted - for objects, which will be deleted from database.
