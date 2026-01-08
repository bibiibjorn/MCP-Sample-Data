"""
PII pattern definitions for detecting personally identifiable information.

Includes regex patterns, column name heuristics, and validation functions
for various PII types including emails, phones, SSNs, names, addresses, etc.
"""

import re
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Callable, Set


class PIIType(Enum):
    """Types of personally identifiable information"""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    NAME = "name"
    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    FULL_NAME = "full_name"
    ADDRESS = "address"
    STREET_ADDRESS = "street_address"
    CITY = "city"
    STATE = "state"
    ZIP_CODE = "zip_code"
    COUNTRY = "country"
    PASSPORT = "passport"
    DRIVERS_LICENSE = "drivers_license"
    BANK_ACCOUNT = "bank_account"
    IBAN = "iban"
    MEDICAL_ID = "medical_id"
    NATIONAL_ID = "national_id"
    USERNAME = "username"
    PASSWORD = "password"
    GENDER = "gender"
    AGE = "age"
    SALARY = "salary"
    ETHNICITY = "ethnicity"
    RELIGION = "religion"
    POLITICAL = "political"
    BIOMETRIC = "biometric"
    GENETIC = "genetic"


class PIISensitivity(Enum):
    """Sensitivity levels for PII"""
    LOW = "low"           # Indirectly identifying (city, state)
    MEDIUM = "medium"     # Potentially identifying (name, DOB)
    HIGH = "high"         # Directly identifying (email, phone)
    CRITICAL = "critical" # Highly sensitive (SSN, credit card)


@dataclass
class PIIPattern:
    """Pattern definition for detecting PII"""
    pii_type: PIIType
    sensitivity: PIISensitivity
    regex_pattern: Optional[str] = None
    column_name_patterns: List[str] = field(default_factory=list)
    validation_func: Optional[Callable[[str], bool]] = None
    description: str = ""
    gdpr_category: str = ""  # GDPR Article 9 special categories


# Validation functions
def validate_luhn(number: str) -> bool:
    """Luhn algorithm for credit card validation"""
    digits = [int(d) for d in re.sub(r'\D', '', number)]
    if len(digits) < 13:
        return False
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def validate_ssn(ssn: str) -> bool:
    """Validate SSN format (not checking against real SSN database)"""
    ssn_clean = re.sub(r'\D', '', ssn)
    if len(ssn_clean) != 9:
        return False
    # Check for invalid SSNs
    area = int(ssn_clean[:3])
    group = int(ssn_clean[3:5])
    serial = int(ssn_clean[5:])
    if area == 0 or area == 666 or area >= 900:
        return False
    if group == 0 or serial == 0:
        return False
    return True


def validate_iban(iban: str) -> bool:
    """Validate IBAN using mod 97"""
    iban_clean = re.sub(r'\s', '', iban).upper()
    if len(iban_clean) < 15 or len(iban_clean) > 34:
        return False
    # Move first 4 chars to end
    rearranged = iban_clean[4:] + iban_clean[:4]
    # Convert letters to numbers (A=10, B=11, etc.)
    numeric = ''
    for char in rearranged:
        if char.isdigit():
            numeric += char
        else:
            numeric += str(ord(char) - ord('A') + 10)
    try:
        return int(numeric) % 97 == 1
    except ValueError:
        return False


def validate_email(email: str) -> bool:
    """Basic email format validation"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))


def validate_phone(phone: str) -> bool:
    """Validate phone number has reasonable format"""
    digits = re.sub(r'\D', '', phone)
    return 7 <= len(digits) <= 15


# Pattern definitions
PII_PATTERNS: List[PIIPattern] = [
    # CRITICAL SENSITIVITY
    PIIPattern(
        pii_type=PIIType.SSN,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
        column_name_patterns=['ssn', 'social_security', 'socialsecurity', 'social_sec', 'ssnum', 'ss_number'],
        validation_func=validate_ssn,
        description="US Social Security Number",
        gdpr_category="national_id"
    ),
    PIIPattern(
        pii_type=PIIType.CREDIT_CARD,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b(?:\d{4}[-\s]?){3}\d{4}\b|\b\d{15,16}\b',
        column_name_patterns=['credit_card', 'creditcard', 'card_number', 'cardnumber', 'cc_num', 'ccnum', 'pan'],
        validation_func=validate_luhn,
        description="Credit/Debit Card Number",
        gdpr_category="financial"
    ),
    PIIPattern(
        pii_type=PIIType.BANK_ACCOUNT,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b\d{8,17}\b',  # Broad pattern, relies on column names
        column_name_patterns=['bank_account', 'bankaccount', 'account_number', 'accountnumber', 'acct_num', 'routing_number'],
        description="Bank Account Number",
        gdpr_category="financial"
    ),
    PIIPattern(
        pii_type=PIIType.IBAN,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b',
        column_name_patterns=['iban', 'int_bank_account'],
        validation_func=validate_iban,
        description="International Bank Account Number",
        gdpr_category="financial"
    ),
    PIIPattern(
        pii_type=PIIType.PASSPORT,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b[A-Z]{1,2}\d{6,9}\b',
        column_name_patterns=['passport', 'passport_number', 'passportnum', 'passport_no'],
        description="Passport Number",
        gdpr_category="national_id"
    ),
    PIIPattern(
        pii_type=PIIType.DRIVERS_LICENSE,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b[A-Z0-9]{5,15}\b',  # Varies by state/country
        column_name_patterns=['drivers_license', 'driverslicense', 'dl_number', 'dlnum', 'license_number', 'driving_license'],
        description="Driver's License Number",
        gdpr_category="national_id"
    ),
    PIIPattern(
        pii_type=PIIType.MEDICAL_ID,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b[A-Z0-9]{6,15}\b',
        column_name_patterns=['medical_id', 'medicalid', 'patient_id', 'patientid', 'mrn', 'medical_record', 'npi'],
        description="Medical/Patient ID",
        gdpr_category="health"
    ),
    PIIPattern(
        pii_type=PIIType.NATIONAL_ID,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=r'\b[A-Z0-9]{6,15}\b',
        column_name_patterns=['national_id', 'nationalid', 'national_insurance', 'ni_number', 'nino', 'tax_id', 'tin'],
        description="National ID Number",
        gdpr_category="national_id"
    ),

    # HIGH SENSITIVITY
    PIIPattern(
        pii_type=PIIType.EMAIL,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
        column_name_patterns=['email', 'email_address', 'emailaddress', 'e_mail', 'mail', 'contact_email'],
        validation_func=validate_email,
        description="Email Address",
        gdpr_category="contact"
    ),
    PIIPattern(
        pii_type=PIIType.PHONE,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=r'\b(?:\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
        column_name_patterns=['phone', 'phone_number', 'phonenumber', 'telephone', 'tel', 'mobile', 'cell', 'contact_phone', 'fax'],
        validation_func=validate_phone,
        description="Phone Number",
        gdpr_category="contact"
    ),
    PIIPattern(
        pii_type=PIIType.IP_ADDRESS,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
        column_name_patterns=['ip', 'ip_address', 'ipaddress', 'ip_addr', 'client_ip', 'user_ip'],
        description="IP Address",
        gdpr_category="online_identifier"
    ),
    PIIPattern(
        pii_type=PIIType.STREET_ADDRESS,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=r'\b\d+\s+[\w\s]+(?:street|st|avenue|ave|road|rd|drive|dr|lane|ln|way|court|ct|boulevard|blvd)\b',
        column_name_patterns=['address', 'street_address', 'streetaddress', 'address1', 'address_line', 'mailing_address', 'home_address'],
        description="Street Address",
        gdpr_category="location"
    ),
    PIIPattern(
        pii_type=PIIType.USERNAME,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=None,  # No reliable regex for usernames
        column_name_patterns=['username', 'user_name', 'userid', 'user_id', 'login', 'login_name', 'screen_name', 'handle'],
        description="Username/User ID",
        gdpr_category="online_identifier"
    ),
    PIIPattern(
        pii_type=PIIType.PASSWORD,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=None,  # Should never be stored in plaintext
        column_name_patterns=['password', 'passwd', 'pwd', 'pass', 'secret', 'pin', 'passcode'],
        description="Password/Secret",
        gdpr_category="security"
    ),

    # MEDIUM SENSITIVITY
    PIIPattern(
        pii_type=PIIType.FIRST_NAME,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=None,  # Too many false positives
        column_name_patterns=['first_name', 'firstname', 'fname', 'given_name', 'givenname', 'forename'],
        description="First Name",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.LAST_NAME,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=None,
        column_name_patterns=['last_name', 'lastname', 'lname', 'surname', 'family_name', 'familyname'],
        description="Last Name",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.FULL_NAME,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=None,
        column_name_patterns=['name', 'full_name', 'fullname', 'customer_name', 'employee_name', 'contact_name', 'person_name'],
        description="Full Name",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.DATE_OF_BIRTH,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',
        column_name_patterns=['dob', 'date_of_birth', 'dateofbirth', 'birth_date', 'birthdate', 'birthday'],
        description="Date of Birth",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.AGE,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=r'\b(?:[1-9]|[1-9]\d|1[01]\d|120)\b',  # 1-120
        column_name_patterns=['age', 'patient_age', 'customer_age', 'age_years'],
        description="Age",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.GENDER,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=r'\b(?:male|female|m|f|man|woman|non-binary|other)\b',
        column_name_patterns=['gender', 'sex', 'gender_identity'],
        description="Gender",
        gdpr_category="personal"
    ),
    PIIPattern(
        pii_type=PIIType.SALARY,
        sensitivity=PIISensitivity.MEDIUM,
        regex_pattern=r'\b\d{4,8}(?:\.\d{2})?\b',
        column_name_patterns=['salary', 'wage', 'compensation', 'pay', 'income', 'annual_salary', 'hourly_rate'],
        description="Salary/Income",
        gdpr_category="financial"
    ),
    PIIPattern(
        pii_type=PIIType.ZIP_CODE,
        sensitivity=PIISensitivity.LOW,
        regex_pattern=r'\b\d{5}(?:-\d{4})?\b',  # US ZIP
        column_name_patterns=['zip', 'zip_code', 'zipcode', 'postal_code', 'postalcode', 'postcode'],
        description="ZIP/Postal Code",
        gdpr_category="location"
    ),

    # LOW SENSITIVITY (but still PII)
    PIIPattern(
        pii_type=PIIType.CITY,
        sensitivity=PIISensitivity.LOW,
        regex_pattern=None,  # Too many false positives
        column_name_patterns=['city', 'town', 'municipality', 'city_name'],
        description="City",
        gdpr_category="location"
    ),
    PIIPattern(
        pii_type=PIIType.STATE,
        sensitivity=PIISensitivity.LOW,
        regex_pattern=None,
        column_name_patterns=['state', 'province', 'region', 'state_code', 'state_name'],
        description="State/Province",
        gdpr_category="location"
    ),
    PIIPattern(
        pii_type=PIIType.COUNTRY,
        sensitivity=PIISensitivity.LOW,
        regex_pattern=None,
        column_name_patterns=['country', 'country_code', 'country_name', 'nation'],
        description="Country",
        gdpr_category="location"
    ),

    # SPECIAL CATEGORIES (GDPR Article 9)
    PIIPattern(
        pii_type=PIIType.ETHNICITY,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=None,
        column_name_patterns=['ethnicity', 'race', 'ethnic_origin', 'racial_origin'],
        description="Ethnicity/Race",
        gdpr_category="special_category"
    ),
    PIIPattern(
        pii_type=PIIType.RELIGION,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=None,
        column_name_patterns=['religion', 'religious_affiliation', 'faith', 'belief'],
        description="Religion",
        gdpr_category="special_category"
    ),
    PIIPattern(
        pii_type=PIIType.POLITICAL,
        sensitivity=PIISensitivity.HIGH,
        regex_pattern=None,
        column_name_patterns=['political_party', 'political_affiliation', 'political_view'],
        description="Political Affiliation",
        gdpr_category="special_category"
    ),
    PIIPattern(
        pii_type=PIIType.BIOMETRIC,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=None,
        column_name_patterns=['fingerprint', 'face_id', 'biometric', 'iris_scan', 'voice_print'],
        description="Biometric Data",
        gdpr_category="special_category"
    ),
    PIIPattern(
        pii_type=PIIType.GENETIC,
        sensitivity=PIISensitivity.CRITICAL,
        regex_pattern=None,
        column_name_patterns=['genetic', 'dna', 'genome', 'genetic_marker'],
        description="Genetic Data",
        gdpr_category="special_category"
    ),
]


# Create lookup dictionaries for fast access
PII_BY_TYPE: dict[PIIType, PIIPattern] = {p.pii_type: p for p in PII_PATTERNS}
COLUMN_NAME_TO_PII: dict[str, PIIType] = {}
for pattern in PII_PATTERNS:
    for col_pattern in pattern.column_name_patterns:
        COLUMN_NAME_TO_PII[col_pattern.lower()] = pattern.pii_type


def detect_pii_in_value(value: str, pii_types: Optional[Set[PIIType]] = None) -> List[tuple[PIIType, float]]:
    """
    Detect PII in a single value.

    Args:
        value: String value to check
        pii_types: Optional set of PII types to check (all if None)

    Returns:
        List of (PIIType, confidence) tuples
    """
    if not value or not isinstance(value, str):
        return []

    detections = []
    value_clean = value.strip()

    for pattern in PII_PATTERNS:
        if pii_types and pattern.pii_type not in pii_types:
            continue

        if not pattern.regex_pattern:
            continue

        if re.search(pattern.regex_pattern, value_clean, re.IGNORECASE):
            confidence = 0.7  # Base confidence for regex match

            # Boost confidence if validation passes
            if pattern.validation_func:
                if pattern.validation_func(value_clean):
                    confidence = 0.95
                else:
                    confidence = 0.3  # Lower confidence if validation fails

            detections.append((pattern.pii_type, confidence))

    return detections


def get_pii_column_candidates(column_name: str) -> List[tuple[PIIType, float]]:
    """
    Check if column name suggests PII content.

    Args:
        column_name: Name of the column

    Returns:
        List of (PIIType, confidence) tuples
    """
    candidates = []
    name_lower = column_name.lower().replace('-', '_').replace(' ', '_')

    for pattern in PII_PATTERNS:
        for col_pattern in pattern.column_name_patterns:
            # Exact match
            if name_lower == col_pattern:
                candidates.append((pattern.pii_type, 0.9))
                break
            # Contains match
            elif col_pattern in name_lower or name_lower in col_pattern:
                candidates.append((pattern.pii_type, 0.7))
                break
            # Fuzzy match (starts with or ends with)
            elif name_lower.startswith(col_pattern) or name_lower.endswith(col_pattern):
                candidates.append((pattern.pii_type, 0.6))
                break

    return candidates
