# from apps.contact_matching_service_v3.api.models.db_models import *
# from config.cms_config import *
from sqlalchemy import or_, and_, func
import logging
from uuid import uuid4
from sqlalchemy import func, and_, or_

# from apps.contact_matching_service_v3.api.controller.bulk_validations import (
#     query_based_on_type,
# )
# from apps.contact_matching_service_v3.api.controller.mapper_utilities import *
# from apps.contact_matching_service_v3.api.controller.common_utilities import *


# changes in this func
def match_client_type_contact_ids(job_id: str, session):
    """
    Function to match salesforce/app/dealer type contacts in Contact Identifier Table.
    The function queries contacts of each type and performs sequential matching
    of the fields to be matched for that type.
    If match found:
        Status of contact in Contact Staging Table is updated to "MATCHED"
    """
    lstclient = [
        ClientType.salesforce.value,
        ClientType.app.value,
        ClientType.dealer.value,
    ]
    try:
        for client in lstclient:
            matched_contact_list = list()
            for _, identifier_group, identifier in get_matching_fields(client):
                identifier_list = identifier.split("+")
                if len(identifier_list) == 1:
                    matched_contacts = (
                        session.query(ContactStagingTable, ContactIdentifierTable)
                        .filter(ContactStagingTable.jobid__c == job_id)
                        .filter(ContactStagingTable.source_type__c == client)
                        .filter(
                            ContactStagingTable.status__c
                            == RecordStatus.validated.value
                        )
                        .filter(
                            and_(
                                getattr(
                                    ContactIdentifierTable,
                                    IdentifierColumns.identifier.value,
                                )
                                == getattr(ContactStagingTable, identifier),
                                getattr(
                                    ContactIdentifierTable,
                                    IdentifierColumns.identifier_group.value,
                                )
                                == identifier_group,
                            )
                        )
                        .all()
                    )
                else:
                    matched_contacts = (
                        session.query(ContactStagingTable, ContactIdentifierTable)
                        .filter(ContactStagingTable.jobid__c == job_id)
                        .filter(ContactStagingTable.source_type__c == client)
                        .filter(
                            ContactStagingTable.status__c
                            == RecordStatus.validated.value
                        )
                        .filter(
                            and_(
                                getattr(
                                    ContactIdentifierTable,
                                    IdentifierColumns.identifier.value,
                                )
                                == func.concat(
                                    getattr(ContactStagingTable, identifier_list[0]),
                                    getattr(ContactStagingTable, identifier_list[1]),
                                ),
                                getattr(
                                    ContactIdentifierTable,
                                    IdentifierColumns.identifier_group.value,
                                )
                                == identifier_group,
                            )
                        )
                        .all()
                    )

                matched_contact_list = matched_contact_list + matched_contacts
                print(f"==matched_contact_list=== {matched_contacts}")
                staged_identified = dict()
                """
                 Create a dictionary which stores the contact identifiers
                 as a list for each staging record

                """

                for staged_contact, identified_contact in matched_contacts:
                    if staged_contact.id not in staged_identified:
                        staged_identified[staged_contact.id] = list()
                    staged_identified[staged_contact.id].append(identified_contact.id)
                print(f"==staged_identified dictionary======{staged_identified}")

                for staged_contact, identified_contact in matched_contacts:
                    print(f"length {len(staged_identified[staged_contact.id])}")
                    print(f"==stage identifier=={staged_identified[staged_contact.id]}")
                    print(f"==staged_contact.status__c=={staged_contact.status__c}")
                    general_matm_owner = get_general_matm_owner(client)
                    if (
                        len(staged_identified[staged_contact.id]) > 1
                        and staged_contact.status__c == RecordStatus.validated.value
                    ):
                        # check for matm owner matches for the set of matched identifier records
                        for identifier_id in staged_identified[staged_contact.id]:
                            print(f"====identifier_id====={identifier_id}")
                            single_identified_contact = (
                                session.query(ContactIdentifierTable)
                                .filter(ContactIdentifierTable.id == identifier_id)
                                .first()
                            )

                            if (
                                single_identified_contact.matm_owner__c
                                == staged_contact.source_name__c
                                and staged_contact.status__c
                                == RecordStatus.validated.value
                            ):
                                staged_contact.matched_contact_id__c = (
                                    single_identified_contact.contact_id__c
                                )
                                staged_contact.status__c = RecordStatus.matched.value
                                main_contacts = (
                                    session.query(ContactTable)
                                    .filter(
                                        ContactTable.sfid
                                        == single_identified_contact.contact_id__c
                                    )
                                    .first()
                                )
                                if (
                                    single_identified_contact.contact_id__c is not None
                                    and main_contacts is not None
                                ):
                                    staged_contact.matched_individual_id__c = (
                                        main_contacts.individualid
                                    )
                                else:
                                    staged_contact.status__c = (
                                        RecordStatus.matcherror.value
                                    )
                                    staged_contact.status_description__c = (
                                        f"{MATCHING_ERROR}"
                                    )

                        # check for MA General matches for the set of matched identifier records if no match found above
                        if staged_contact.status__c == RecordStatus.validated.value:
                            for identifier_id in staged_identified[staged_contact.id]:
                                ma_general_contact = (
                                    session.query(ContactIdentifierTable)
                                    .filter(ContactIdentifierTable.id == identifier_id)
                                    .filter(
                                        ContactIdentifierTable.matm_owner__c
                                        == general_matm_owner
                                    )
                                    .first()
                                )
                                print(f"====ma general contact===={ma_general_contact}")
                                if (
                                    ma_general_contact is not None
                                    and ma_general_contact.contact_id__c is not None
                                ):
                                    # if len(ma_general_contact) >= 1:

                                    staged_contact.matched_contact_id__c = (
                                        ma_general_contact.contact_id__c
                                    )
                                    print(
                                        f"===idenfier contact id={identified_contact.contact_id__c}"
                                    )
                                    main_contacts = (
                                        session.query(ContactTable)
                                        .filter(
                                            ContactTable.sfid
                                            == ma_general_contact.contact_id__c
                                        )
                                        .first()
                                    )
                                    if main_contacts is not None:
                                        staged_contact.matched_individual_id__c = (
                                            main_contacts.individualid
                                        )
                                        staged_contact.status__c = (
                                            RecordStatus.partialmatch.value
                                        )
                                        print(
                                            f"==staged_contact==status====={staged_contact.status__c}"
                                        )
                            if staged_contact.status__c == RecordStatus.validated.value:
                                staged_contact.matched_individual_id__c = ""
                                staged_contact.status__c = RecordStatus.matcherror.value
                                staged_contact.status_description__c = (
                                    f"{MATCHING_ERROR}"
                                )
                                print(
                                    f"====NO MA general======={staged_contact.status__c}"
                                )

                    else:
                        if staged_contact.status__c == RecordStatus.validated.value:
                            if (
                                identified_contact.matm_owner__c
                                == staged_contact.source_name__c
                            ):
                                staged_contact.matched_contact_id__c = (
                                    identified_contact.contact_id__c
                                )
                                staged_contact.status__c = RecordStatus.matched.value
                                main_contacts = (
                                    session.query(ContactTable)
                                    .filter(
                                        ContactTable.sfid
                                        == identified_contact.contact_id__c
                                    )
                                    .first()
                                )
                                if main_contacts is not None:
                                    staged_contact.matched_individual_id__c = (
                                        main_contacts.individualid
                                    )
                                else:
                                    staged_contact.status__c = (
                                        RecordStatus.matcherror.value
                                    )
                                    staged_contact.status_description__c = (
                                        f"{MATCHING_ERROR}"
                                    )
                            elif identified_contact.matm_owner__c == general_matm_owner:
                                if identified_contact.contact_id__c is not None:
                                    staged_contact.matched_contact_id__c = (
                                        identified_contact.contact_id__c
                                    )
                                    main_contacts = (
                                        session.query(ContactTable)
                                        .filter(
                                            ContactTable.sfid
                                            == identified_contact.contact_id__c
                                        )
                                        .first()
                                    )
                                    if main_contacts is not None:
                                        staged_contact.matched_individual_id__c = (
                                            main_contacts.individualid
                                        )
                                        staged_contact.status__c = (
                                            RecordStatus.partialmatch.value
                                        )
                                        log(
                                            f"==staged_contact==status====={staged_contact.status__c}"
                                        )
                                    else:
                                        staged_contact.status__c = (
                                            RecordStatus.matcherror.value
                                        )
                                        staged_contact.status_description__c = (
                                            f"{MATCHING_ERROR}"
                                        )

                            else:
                                staged_contact.status__c = RecordStatus.matcherror.value
                                staged_contact.status_description__c = (
                                    f"{MATCHING_ERROR}"
                                )

        stagerecords = session.query(ContactStagingTable).filter(
            ContactStagingTable.jobid__c == job_id
        )
        for records in stagerecords:
            if records.status__c == RecordStatus.validated.value:
                records.status__c = RecordStatus.unmatched.value

        session.flush()
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"{e}")


def update_validated_staged_records(job_id, session):
    """
    Function to Update validated records in Contact Staging Table with status as unmatched
    """
    try:
        validated_contacts = (
            session.query(ContactStagingTable)
            .filter(ContactStagingTable.jobid__c == job_id)
            .filter(ContactStagingTable.status__c == RecordStatus.validated.value)
        )
        for validated_data in validated_contacts:
            validated_data.status__c = RecordStatus.unmatched.value
        session.flush()
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"{e}")


# changes in this func
def update_client_type(session):
    """
    Function to Update new records in Contact Staging Table with source type and source name
    from Organisation Source Table.
    """
    try:
        job_id = uuid4().hex
        unmatched_data = (
            session.query(ContactStagingTable, OrganizationSourceTable)
            .filter(
                or_(
                    func.lower(
                        getattr(ContactStagingTable, StagingColumns.status.value)
                    )
                    == RecordStatus.inserted.value.lower(),
                    func.lower(
                        getattr(ContactStagingTable, StagingColumns.status.value)
                    )
                    == RecordStatus.uploaded.value.lower(),
                )
            )
            .filter(
                or_(
                    ContactStagingTable.organisationsourceid__c
                    == OrganizationSourceTable.client_id__c,
                    ContactStagingTable.client_id__c
                    == OrganizationSourceTable.client_id__c,
                )
            )
            .limit(100)
            .all()
        )
        if len(unmatched_data) > 0:
            for staged_data, org_data in unmatched_data:
                staged_data.source_type__c = org_data.client_type__c
                staged_data.source_name__c = org_data.secondary_matmowner__c
                staged_data.primary_matmowner__c = org_data.primary_matmowner__c
                staged_data.primary_recordtypeid__c = org_data.primary_recordtypeid__c
                staged_data.secondary_matmowner__c = org_data.secondary_matmowner__c
                staged_data.secondary_recordtypeid__c = (
                    org_data.secondary_recordtypeid__c
                )
                staged_data.isseparatecontact__c = org_data.is_separate_contact__c
                staged_data.is_obfuscated__c = org_data.is_obfuscated__c

                # staged_data.status__c = RecordStatus.unmatched.value
                staged_data.jobid__c = job_id
            session.flush()
            session.commit()
            return job_id
        else:
            return None
    except Exception as e:
        session.rollback()
        print(f"{e}")


def match_contact_ids():
    """
    Function to initiate
    """
    try:
        # session = SessionLocal()
        job_id = update_client_type(session)
        if job_id is not None:
            query_based_on_type(job_id, session)
            set_consent_data(job_id, session)
            # update_validated_staged_records(job_id, session)
            match_client_type_contact_ids(job_id, session)
        session.close()
        return job_id
    except Exception as e:
        print(f"{e}")


def set_consent_data(job_id: str, session):
    try:
        print("in set consent data")
        staged_contacts = session.query(ContactStagingTable).filter(
            ContactStagingTable.jobid__c == job_id
        )

        for stage_contact in staged_contacts:
            if stage_contact.is_cat_consent__c is True:
                print(f"is cat consent{stage_contact.is_cat_consent__c}")
                if (
                    stage_contact.countrycode__c in ["GE", "Germany"]
                    and stage_contact.emailoptinstatus__c is True
                ):
                    stage_contact.consent_level_summary__c = CONSENT_PENDING_VALIDATED
                elif (
                    stage_contact.countrycode__c not in ["GE", "Germany"]
                    and stage_contact.emailoptinstatus__c is True
                ):
                    stage_contact.consent_level_summary__c = CONSENT_EXPRESS
                elif stage_contact.emailoptinstatus__c is False:
                    stage_contact.consent_level_summary__c = REVOKED_EXPRESS
                elif (
                    stage_contact.countrycode__c
                    not in ["USA", "US", "United States of America"]
                    or stage_contact.countrycode__c is None
                ) and stage_contact.emailoptinstatus__c is False:
                    stage_contact.consent_level_summary__c = NO_CONSENT
            else:
                print("no consent")
                stage_contact.consent_level_summary__c = NO_CONSENT
        session.flush()
        session.commit()

    except Exception as e:
        session.rollback()
        print(f"{e}")


def create_obfuscated_records(
    staged_contact_data, matm_owner, record_type_id, session, sfdc_call
):
    try:
        if (
            staged_contact_data.status__c == RecordStatus.partialmatch.value
            or staged_contact_data.status__c == RecordStatus.unmatched.value
        ):
            if staged_contact_data.status__c == RecordStatus.partialmatch.value:
                unique_individual_id = staged_contact_data.matched_individual_id__c
            else:
                unique_individual_id = uuid4().hex[:18]
                individual_data = map_obfuscated_individual_data(
                    staged_contact_data, unique_individual_id
                )
                session.add(IndividualTable(**individual_data))
                session.flush()

            unique_contact_id = uuid4().hex[:18]
            contact_data = map_obfuscated_contact_data(
                staged_contact_data,
                unique_individual_id,
                unique_contact_id,
                matm_owner,
                record_type_id,
            )

            session.add(ContactTable(**contact_data))

            if sfdc_call:
                sfid = sfdc_api_call(
                    staged_contact_data, unique_contact_id, matm_owner, record_type_id
                )

            unique_contact_source_id = uuid4().hex[:18]
            contact_source_data = map_obfuscated_contact_source_data(
                staged_contact_data, unique_contact_source_id, unique_contact_id
            )
            session.add(ContactSourceTable(**contact_source_data))

            for (
                identifier_type,
                identifier_group,
                column_name,
                is_sms_capable,
                is_business_phone,
                phone_type,
                capture_source,
                capture_contact_point_type,
                consent_date,
                opt_in_status,
            ) in get_creation_fields(
                ClientType[staged_contact_data.source_type__c.strip().lower()]
            ):
                if column_name == "sourceid__c":

                    unique_contact_identifier_id = uuid4().hex[:18]
                    contact_identifier_data = map_obfuscated_contact_identifier_data(
                        staged_contact_data,
                        column_name,
                        RecordStatus.active.value.upper(),
                        identifier_type,
                        identifier_group,
                        matm_owner,
                        RecordStatus.new.value.upper(),
                        unique_contact_id,
                        unique_contact_identifier_id,
                    )
                    session.add(ContactIdentifierTable(**contact_identifier_data))

                    contact_source_identifier_data = map_obfuscated_contact_source_identifier(
                        unique_contact_id,
                        unique_contact_identifier_id,
                        unique_contact_source_id,
                    )
                    session.add(
                        ContactSourceIdentifierTable(**contact_source_identifier_data)
                    )

            return unique_individual_id, unique_contact_id, sfid

    except Exception as e:
        logging.info(f"{e}")


def create_contact(
    staged_contact_data,
    unique_individual_id,
    matm_owner,
    record_type_id,
    session,
    sfdc_call,
):
    try:
        sfid = None
        unique_contact_id = uuid4().hex[:18]
        contact_data = map_contact_data(
            staged_contact_data,
            matm_owner,
            record_type_id,
            RecordStatus.active.value.upper(),
            unique_contact_id,
            unique_individual_id,
        )
        session.add(ContactTable(**contact_data))

        if sfdc_call:
            sfid = sfdc_api_call(
                staged_contact_data, unique_contact_id, matm_owner, record_type_id
            )

        unique_contact_source_id = uuid4().hex[:18]
        contact_source_data = map_contact_source_data(
            staged_contact_data,
            RecordStatus.processed.value.upper(),
            RecordStatus.active.value.upper(),
            unique_contact_source_id,
            unique_contact_id,
        )

        if (
            staged_contact_data.source_type__c.strip().lower()
            == ClientType.dealer.value.strip().lower()
        ):
            contact_source_data.update(
                source_id__c=staged_contact_data.contactid__c,
                dcn__c=staged_contact_data.dcn__c,
            )
            staged_contact_data.dc_dcn__c = (
                f"{staged_contact_data.dealercode__c}{staged_contact_data.dcn__c}"
            )
        session.add(ContactSourceTable(**contact_source_data))

        for (
            identifier_type,
            identifier_group,
            column_name,
            is_sms_capable,
            is_business_phone,
            phone_type,
            capture_source,
            capture_contact_point_type,
            consent_date,
            opt_in_status,
        ) in get_creation_fields(
            ClientType[staged_contact_data.source_type__c.strip().lower()]
        ):

            if getattr(staged_contact_data, column_name) is not None:

                unique_contact_identifier_id = uuid4().hex[:18]
                contact_identifier_data = map_contact_identifier_data(
                    staged_contact_data,
                    column_name,
                    RecordStatus.active.value.upper(),
                    identifier_type,
                    identifier_group,
                    matm_owner,
                    RecordStatus.new.value.upper(),
                    unique_contact_id,
                    unique_contact_identifier_id,
                )

                contact_source_identifier_data = map_contact_source_identifier(
                    unique_contact_id,
                    unique_contact_identifier_id,
                    unique_contact_source_id,
                )

                if (
                    identifier_group.lower() == Identifiers.email.value.lower()
                    or identifier_group.lower() == Identifiers.phone.value.lower()
                    or identifier_group.lower() == Identifiers.mobile.value.lower()
                ):

                    contact_identifier_data.update(
                        status_time__c=getattr(staged_contact_data, consent_date)
                    )

                    if identifier_group.lower() == Identifiers.email.value.lower():
                        unique_email_contact_point_id = uuid4().hex[:18]

                        email_contact_point_data = map_contact_point_email_data(
                            staged_contact_data,
                            matm_owner,
                            opt_in_status,
                            unique_contact_id,
                            unique_email_contact_point_id,
                        )

                        session.add(ContactPointEmailTable(**email_contact_point_data))
                        session.flush()

                    if (
                        identifier_group.lower() == Identifiers.phone.value.lower()
                        or identifier_group.lower() == Identifiers.mobile.value.lower()
                    ):
                        unique_mobile_contact_point_id = uuid4().hex[:18]
                        contact_point_data_common = map_contact_point_phone_data(
                            staged_contact_data,
                            column_name,
                            matm_owner,
                            is_sms_capable,
                            is_business_phone,
                            phone_type,
                            opt_in_status,
                            unique_contact_id,
                            unique_mobile_contact_point_id,
                        )

                        session.add(ContactPointPhoneTable(**contact_point_data_common))

                    contact_point_consent_data = map_contact_point_consent_data(
                        staged_contact_data,
                        identifier_group,
                        column_name,
                        consent_date,
                        opt_in_status,
                        matm_owner,
                        capture_source,
                        capture_contact_point_type,
                        unique_contact_id,
                        unique_individual_id,
                    )

                    session.add(ContactPointConsentTable(**contact_point_consent_data))

                session.add(ContactIdentifierTable(**contact_identifier_data))

                session.add(
                    ContactSourceIdentifierTable(**contact_source_identifier_data)
                )

        return unique_contact_id, sfid
    except Exception as e:
        logging.info(f"Error in cerating contact and child records: {e}")


def create_records(staged_contact_data, sfdc_call, create_individual):
    try:
        engine.dispose()
        # session = SessionLocal()

        record_types = [
            (
                staged_contact_data.primary_matmowner__c,
                staged_contact_data.primary_recordtypeid__c,
            ),
            (
                staged_contact_data.secondary_matmowner__c,
                staged_contact_data.secondary_recordtypeid__c,
            ),
        ]

        if staged_contact_data.is_obfuscated__c:
            unique_individual_id, unique_contact_id, sfid = create_obfuscated_records(
                staged_contact_data,
                record_types[1][0],
                record_types[1][1],
                session,
                sfdc_call,
            )

            session.query(ContactStagingTable).filter(
                func.lower(
                    getattr(ContactStagingTable, StagingColumns.unique_record_key.value)
                )
                == staged_contact_data.stagecontactkey__c.lower()
            ).update(
                dict(
                    status__c=RecordStatus.created.value,
                    created_contact_id__c=unique_contact_id,
                    created_individual_id__c=unique_individual_id,
                ),
                synchronize_session=False,
            )
            session.commit()
            session.close()
            return unique_individual_id, sfid

        if create_individual:
            unique_individual_id = uuid4().hex[:18]
            individual_data = map_individual_data(
                staged_contact_data, unique_individual_id
            )

            session.add(IndividualTable(**individual_data))
            session.flush()
        else:
            unique_individual_id = staged_contact_data.matched_individual_id__c

        if staged_contact_data.isseparatecontact__c:
            for matm_owner, record_type_id in record_types:
                unique_contact_id, sfid = create_contact(
                    staged_contact_data,
                    unique_individual_id,
                    matm_owner,
                    record_type_id,
                    session,
                    sfdc_call,
                )
        else:
            unique_contact_id, sfid = create_contact(
                staged_contact_data,
                unique_individual_id,
                record_types[0][0],
                record_types[0][1],
                session,
                sfdc_call,
            )

        session.query(ContactStagingTable).filter(
            func.lower(
                getattr(ContactStagingTable, StagingColumns.unique_record_key.value)
            )
            == staged_contact_data.stagecontactkey__c.lower()
        ).update(
            dict(
                status__c=RecordStatus.created.value,
                created_contact_id__c=unique_contact_id,
                created_individual_id__c=unique_individual_id,
            ),
            synchronize_session=False,
        )
        session.flush()
        session.commit()
        session.close()
        return unique_individual_id, sfid
    except Exception as e:
        session.rollback()
        logging.info(f"Error in creating records: {e}")


def get_records(record_status, job_id):
    try:
        # session = SessionLocal()
        records = (
            session.query(ContactStagingTable)
            .filter(ContactStagingTable.jobid__c == job_id)
            .filter(
                func.lower(getattr(ContactStagingTable, StagingColumns.status.value))
                == record_status.lower()
            )
            .order_by(ContactStagingTable.id.desc())
            .limit(10)
            .all()
        )
        session.close()
        return records
    except Exception as e:
        session.rollback()
        logging.info(e)


def initiate_creation(job_id):
    try:
        unmatched_records = get_records(RecordStatus.unmatched.value, job_id)
        for staged_contact_data in unmatched_records:
            create_records(staged_contact_data, False, True)
    except Exception as e:
        logging.info(e)


def update_partial_match_contacts(job_id):
    try:
        partialmatch_records = get_records(RecordStatus.partialmatch.value, job_id)
        print(f"partial match {partialmatch_records}")

        if len(partialmatch_records) == 1:
            sfdc_call_flag = True
        else:
            sfdc_call_flag = False

        update_list = []
        unique_individual_id = None
        sfid = None
        for staged_contact_data in partialmatch_records:
            if staged_contact_data.isseparatecontact__c:
                unique_individual_id, sfid = create_records(
                    staged_contact_data, sfdc_call_flag, create_individual=False
                )
            else:
                update_list.append(staged_contact_data)
        # session = SessionLocal()
        update_contacts(session, update_list)
        session.flush()
        session.commit()
        session.close()
        return unique_individual_id, sfid

    except Exception as e:
        session.rollback()
        logging.info(e)


def update_matched_contacts(job_id):
    try:
        # session = SessionLocal()
        matched_records = get_records(RecordStatus.matched.value, job_id)
        print(f"matched {matched_records}")
        update_contacts(session, matched_records)
        session.flush()
        session.commit()
        session.close()
    except Exception as e:
        session.rollback()
        logging.info(e)


def update_contacts(session, matched_records):

    try:
        for staged_contact_data in matched_records:
            print(staged_contact_data.matched_contact_id__c)
            print(staged_contact_data.matched_individual_id__c)
            if staged_contact_data.matched_contact_id__c is not None:
                contact_data = (
                    session.query(ContactTable)
                    .filter(
                        func.lower(ContactTable.sfid)
                        == staged_contact_data.matched_contact_id__c.lower().strip(),
                        func.lower(ContactTable.individualid)
                        == staged_contact_data.matched_individual_id__c.lower().strip(),
                    )
                    .first()
                )
                print("contact data", contact_data.email.strip().lower())
                print(staged_contact_data.email__c.strip().lower())
                if (
                    contact_data.email is not None
                    and contact_data.email.strip().lower()
                    == staged_contact_data.email__c.strip().lower()
                ):

                    contact_data = map_contact_data(
                        staged_contact_data,
                        contact_data.matm_owner__c,
                        contact_data.recordtypeid,
                        RecordStatus.active.value.upper(),
                        contact_data.herokuid__c,
                        contact_data.individual__herokuid__c,
                    )
                    session.query(ContactTable).filter(
                        ContactTable.sfid == staged_contact_data.matched_contact_id__c
                    ).update(contact_data, synchronize_session=False)

                    unique_contact_source_id = uuid4().hex[:18]
                    contact_source_data = map_contact_source_data(
                        staged_contact_data,
                        RecordStatus.processed.value.upper(),
                        RecordStatus.active.value.upper(),
                        unique_contact_source_id,
                        staged_contact_data.matched_contact_id__c,
                    )

                    session.add(ContactSourceTable(**contact_source_data))
                    print("Added")
                    contact_identifiers = (
                        session.query(ContactIdentifierTable)
                        .filter(
                            ContactSourceIdentifierTable.contact_id__c
                            == staged_contact_data.matched_contact_id__c
                        )
                        .all()
                    )
                    for (
                        identifier_type,
                        identifier_group,
                        column_name,
                        is_sms_capable,
                        is_business_phone,
                        phone_type,
                        capture_source,
                        capture_contact_point_type,
                        consent_date,
                        opt_in_status,
                    ) in get_creation_fields(
                        ClientType[staged_contact_data.source_type__c.strip().lower()]
                    ):
                        if getattr(staged_contact_data, column_name) is not None:
                            identifier_found = False
                            for contact_identifier in contact_identifiers:
                                if (identifier_type, identifier_group) == (
                                    contact_identifier.identifier_type__c,
                                    identifier_group__c,
                                ):
                                    identifier_found = True
                                    if (
                                        getattr(staged_contact_data, column_name)
                                        != contact_identifier.identifier__c
                                    ):

                                        setattr(
                                            contact_identifier,
                                            IdentifierColumns.status.value,
                                            RecordStatus.inactive.value,
                                        )

                                        unique_contact_identifier_id = uuid4().hex[:18]
                                        contact_identifier_data = map_contact_identifier_data(
                                            staged_contact_data,
                                            column_name,
                                            RecordStatus.active.value.upper(),
                                            identifier_type,
                                            identifier_group,
                                            contact_identifier.matm_owner__c,
                                            RecordStatus.updated.value.upper(),
                                            staged_contact_data.matched_contact_id__c,
                                            unique_contact_identifier_id,
                                        )

                                        contact_source_identifier_data = map_contact_source_identifier(
                                            staged_contact_data.matched_contact_id__c,
                                            unique_contact_identifier_id,
                                            unique_contact_source_id,
                                        )

                                        if (
                                            identifier_group.lower()
                                            == Identifiers.email.value.lower()
                                            or identifier_group.lower()
                                            == Identifiers.phone.value.lower()
                                            or identifier_group.lower()
                                            == Identifiers.mobile.value.lower()
                                        ):

                                            contact_identifier_data.update(
                                                status_time__c=getattr(
                                                    staged_contact_data, consent_date
                                                )
                                            )

                                        session.add(
                                            ContactIdentifierTable(
                                                **contact_identifier_data
                                            )
                                        )

                                        session.add(
                                            ContactSourceIdentifierTable(
                                                **contact_source_identifier_data
                                            )
                                        )

                            if identifier_found is False:
                                print(identifier_group, column_name)
                                unique_contact_identifier_id = uuid4().hex[:18]
                                contact_identifier_data = map_contact_identifier_data(
                                    staged_contact_data,
                                    column_name,
                                    RecordStatus.active.value.upper(),
                                    identifier_type,
                                    identifier_group,
                                    staged_contact_data.secondary_matmowner__c,
                                    RecordStatus.updated.value.upper(),
                                    staged_contact_data.matched_contact_id__c,
                                    unique_contact_identifier_id,
                                )

                                contact_source_identifier_data = map_contact_source_identifier(
                                    staged_contact_data.matched_contact_id__c,
                                    unique_contact_identifier_id,
                                    unique_contact_source_id,
                                )

                                if (
                                    identifier_group.lower()
                                    == Identifiers.email.value.lower()
                                    or identifier_group.lower()
                                    == Identifiers.phone.value.lower()
                                    or identifier_group.lower()
                                    == Identifiers.mobile.value.lower()
                                ):

                                    contact_identifier_data.update(
                                        status_time__c=getattr(
                                            staged_contact_data, consent_date
                                        )
                                    )

                                session.add(
                                    ContactIdentifierTable(**contact_identifier_data)
                                )

                                session.add(
                                    ContactSourceIdentifierTable(
                                        **contact_source_identifier_data
                                    )
                                )

                            if (
                                identifier_group.lower()
                                == Identifiers.email.value.lower()
                            ):
                                #                                             session.query(ContactPointEmailTable).filter(ContactPointEmailTable.contact_record__c==staged_contact_data.matched_contact_id__c).update(dict(),synchronize_session=False)
                                unique_email_contact_point_id = uuid4().hex[:18]
                                email_contact_point_data = map_contact_point_email_data(
                                    staged_contact_data,
                                    staged_contact_data.secondary_matmowner__c,
                                    opt_in_status,
                                    staged_contact_data.matched_contact_id__c,
                                    unique_email_contact_point_id,
                                )

                                session.add(
                                    ContactPointEmailTable(**email_contact_point_data)
                                )

                            if (
                                identifier_group.lower()
                                == Identifiers.phone.value.lower()
                                or identifier_group.lower()
                                == Identifiers.mobile.value.lower()
                            ):
                                session.query(ContactPointPhoneTable).filter(
                                    ContactPointPhoneTable.contact_record__c
                                    == staged_contact_data.matched_contact_id__c
                                ).update(
                                    dict(activetodate=datetime.datetime.now().date()),
                                    synchronize_session=False,
                                )
                                unique_mobile_contact_point_id = uuid4().hex[:18]
                                contact_point_data_common = map_contact_point_phone_data(
                                    staged_contact_data,
                                    column_name,
                                    staged_contact_data.secondary_matmowner__c,
                                    is_sms_capable,
                                    is_business_phone,
                                    phone_type,
                                    opt_in_status,
                                    staged_contact_data.matched_contact_id__c,
                                    unique_mobile_contact_point_id,
                                )

                                session.add(
                                    ContactPointPhoneTable(**contact_point_data_common)
                                )

                            contact_point_consent_data = map_contact_point_consent_data(
                                staged_contact_data,
                                identifier_group,
                                column_name,
                                consent_date,
                                opt_in_status,
                                staged_contact_data.secondary_matmowner__c,
                                capture_source,
                                capture_contact_point_type,
                                staged_contact_data.matched_contact_id__c,
                                staged_contact_data.matched_individual_id__c,
                            )

                            session.add(
                                ContactPointConsentTable(**contact_point_consent_data)
                            )

                session.query(ContactStagingTable).filter(
                    func.lower(
                        getattr(
                            ContactStagingTable, StagingColumns.unique_record_key.value
                        )
                    )
                    == staged_contact_data.stagecontactkey__c.lower()
                ).update(
                    dict(status__c=RecordStatus.updated.value,),
                    synchronize_session=False,
                )

    except Exception as e:
        print(e)


if __name__ == "__main__":
    job_id = match_contact_ids()
    initiate_creation(job_id)
    # job_id = uuid4().hex
    update_matched_contacts(job_id)
    update_partial_match_contacts(job_id)
