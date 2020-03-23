from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


engine = create_engine("sqlite:///hcms_db", echo=False)
Base = declarative_base(engine)



# class ContactSourceIdentifier(Base):

#     __tablename__ = "salesforce.contact_source_identifier__c"
#     __table_args__ = {"autoload": True}


# class ContactPointConsent(Base):

#     __tablename__ = "salesforce.contactpointconsent"
#     __table_args__ = {"autoload": True}


# class ContactPointEmail(Base):
#     __tablename__ = "salesforce.contactpointemail"
#     __table_args__ = {"autoload": True}


# class ContactPointPhone(Base):
#     __tablename__ = "salesforce.contactpointphone"
#     __table_args__ = {"autoload": True}


class Individual(Base):
    __tablename__ = "salesforce.individual"
    __table_args__ = {"autoload": True}


class Contact(Base):
    __tablename__ = "salesforce.contact"
    __table_args__ = {"autoload": True}


class OrganizationSource(Base):
    __tablename__ = "salesforce.organization_source__c"
    __table_args__ = {"autoload": True}


class StageContact(Base):
    __tablename__ = "salesforce.stage_contact__c"
    __table_args__ = {"autoload": True}


class ContactSource(Base):
    __tablename__ = "salesforce.contact_source__c"
    __table_args__ = {"autoload": True}


class ContactIdentifier(Base):

    __tablename__ = "salesforce.contact_identifier__c"
    __table_args__ = {"autoload": True}


# # ----------------------------------------------------------------------
def loadSession():

    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == "__main__":
    session = loadSession()
