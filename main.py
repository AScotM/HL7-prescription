#!/usr/bin/env python3

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any, Tuple
from enum import Enum
import copy

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

HL7_SEPARATORS = {
    'field': '|',
    'component': '^',
    'subcomponent': '&',
    'repetition': '~',
    'escape': '\\',
}

SEGMENT_DELIMITER = '\r'

class MessageType(Enum):
    ORM = "ORM^O01"
    ORU = "ORU^R01"
    ADT = "ADT^A01"
    RDE = "RDE^O11"

HL7_TABLES = {
    "administrative_sex": {
        "M": "Male",
        "F": "Female",
        "U": "Unknown",
        "A": "Ambiguous",
        "N": "Not applicable",
        "O": "Other"
    },
    "patient_class": {
        "E": "Emergency",
        "I": "Inpatient",
        "O": "Outpatient",
        "P": "Preadmit",
        "R": "Recurring patient",
        "B": "Obstetrics",
        "C": "Commercial Account",
        "N": "Not Applicable",
        "U": "Unknown"
    },
    "order_status": {
        "A": "Some, but not all, results available",
        "CA": "Order was canceled",
        "CM": "Order is completed",
        "DC": "Order was discontinued",
        "ER": "Error, order not found",
        "HD": "Order is on hold",
        "IP": "In process, unspecified",
        "RP": "Order has been replaced",
        "SC": "In process, scheduled"
    },
    "priority": {
        "S": "Stat",
        "A": "ASAP",
        "R": "Routine",
        "P": "Preoperative",
        "C": "Callback",
        "T": "Timing critical"
    },
    "route": {
        "PO": "Oral",
        "IV": "Intravenous",
        "IM": "Intramuscular",
        "SC": "Subcutaneous",
        "INH": "Inhalation",
        "TOP": "Topical",
        "PR": "Rectal",
        "PV": "Vaginal",
        "SL": "Sublingual",
        "BUCC": "Buccal",
        "NAS": "Nasal",
        "OPH": "Ophthalmic",
        "OT": "Otic",
        "TD": "Transdermal",
        "NG": "Nasogastric",
        "GT": "Gastrostomy tube"
    },
    "units_of_measure": {
        "TAB": "Tablet",
        "CAP": "Capsule",
        "ML": "Milliliter",
        "MG": "Milligram",
        "G": "Gram",
        "MCG": "Microgram",
        "L": "Liter",
        "CM": "Centimeter",
        "KG": "Kilogram",
        "MEQ": "Milliequivalent",
        "IU": "International Unit",
        "HR": "Hour",
        "DAY": "Day",
        "WK": "Week",
        "MO": "Month"
    },
    "medication_form": {
        "TAB": "Tablet",
        "CAP": "Capsule",
        "SYR": "Syrup",
        "SUS": "Suspension",
        "INJ": "Injection",
        "CRE": "Cream",
        "OIN": "Ointment",
        "SUP": "Suppository",
        "SOL": "Solution",
        "POW": "Powder",
        "GEL": "Gel",
        "LOT": "Lotion",
        "AER": "Aerosol",
        "PAS": "Paste",
        "FIL": "Film",
        "IMP": "Implant"
    }
}

class HL7EncodingCharacters:
    def __init__(self):
        self.field_separator = HL7_SEPARATORS['field']
        self.component_separator = HL7_SEPARATORS['component']
        self.repetition_separator = HL7_SEPARATORS['repetition']
        self.escape_character = HL7_SEPARATORS['escape']
        self.subcomponent_separator = HL7_SEPARATORS['subcomponent']
    
    def __str__(self):
        return f"{self.field_separator}{self.component_separator}{self.repetition_separator}{self.escape_character}{self.subcomponent_separator}"

class MedicationItem:
    def __init__(
        self,
        medication_code: str,
        medication_name: str,
        form: str,
        strength: str,
        quantity: Decimal,
        unit: str,
        dosage_instruction: str,
        route: str,
        duration_days: Optional[int] = None,
        refills: Optional[int] = None,
        special_instructions: Optional[str] = None,
        substitution_allowed: Optional[bool] = True,
        frequency: Optional[str] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None
    ):
        self.medication_code = medication_code
        self.medication_name = medication_name
        self.form = form
        self.strength = strength
        self.quantity = quantity
        self.unit = unit
        self.dosage_instruction = dosage_instruction
        self.route = route
        self.duration_days = duration_days
        self.refills = refills
        self.special_instructions = special_instructions
        self.substitution_allowed = substitution_allowed
        self.frequency = frequency
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

class PatientInfo:
    def __init__(
        self,
        patient_id: str,
        name: str,
        date_of_birth: date,
        gender: str,
        weight_kg: Optional[Decimal] = None,
        height_cm: Optional[Decimal] = None,
        allergies: Optional[List[str]] = None,
        diagnoses: Optional[List[Tuple[str, str]]] = None
    ):
        self.patient_id = patient_id
        self.name = name
        self.date_of_birth = date_of_birth
        self.gender = gender
        self.weight_kg = weight_kg
        self.height_cm = height_cm
        self.allergies = allergies or []
        self.diagnoses = diagnoses or []

class PrescribingProvider:
    def __init__(
        self,
        id: str,
        name: str,
        qualification: Optional[str] = None,
        specialty: Optional[str] = None,
        contact: Optional[str] = None,
        address: Optional[str] = None
    ):
        self.id = id
        self.name = name
        self.qualification = qualification
        self.specialty = specialty
        self.contact = contact
        self.address = address

class PharmacyInfo:
    def __init__(
        self,
        id: str,
        name: str,
        address: Optional[str] = None,
        contact: Optional[str] = None
    ):
        self.id = id
        self.name = name
        self.address = address
        self.contact = contact

@dataclass
class HL7Config:
    version: str = "2.5"
    message_type: MessageType = MessageType.RDE
    sending_application: str = "PRESCRIPTION_SYSTEM"
    sending_facility: str = "HEALTHCARE_PROVIDER"
    receiving_application: str = "PHARMACY_SYSTEM"
    receiving_facility: str = "PHARMACY"
    charset: str = "UTF-8"
    country_code: str = "USA"
    processing_id: str = "P"
    message_control_id: Optional[str] = None
    include_msh: bool = True
    include_bhs: bool = False
    include_fhs: bool = False
    auto_generate_control_id: bool = True
    max_field_length: int = 200
    escape_xml_chars: bool = True

class HL7GenerationError(Exception):
    def __init__(self, message: str, segment: Optional[str] = None, field: Optional[str] = None):
        self.segment = segment
        self.field = field
        super().__init__(message)

class HL7Segment:
    def __init__(self, segment_id: str, encoding: HL7EncodingCharacters):
        self.segment_id = segment_id
        self.encoding = encoding
        self.fields: List[str] = []
    
    def add_field(self, value: Optional[Any], position: int) -> None:
        while len(self.fields) < position:
            self.fields.append("")
        
        if value is None:
            self.fields.append("")
        else:
            str_value = self._escape_hl7(str(value))
            self.fields.append(str_value)
    
    def set_field(self, value: Optional[Any], position: int) -> None:
        if position < 1:
            raise ValueError("Position must be >= 1")
        
        if len(self.fields) < position:
            self.add_field(value, position)
        else:
            if value is None:
                self.fields[position - 1] = ""
            else:
                self.fields[position - 1] = self._escape_hl7(str(value))
    
    def add_component(self, value: Optional[Any], field_pos: int, comp_pos: int) -> None:
        if field_pos < 1 or comp_pos < 1:
            raise ValueError("Positions must be >= 1")
        
        if len(self.fields) < field_pos:
            self.add_field("", field_pos)
        
        field = self.fields[field_pos - 1]
        components = field.split(self.encoding.component_separator) if field else []
        
        while len(components) < comp_pos:
            components.append("")
        
        components[comp_pos - 1] = self._escape_hl7(str(value)) if value else ""
        self.fields[field_pos - 1] = self.encoding.component_separator.join(components)
    
    def _escape_hl7(self, value: str) -> str:
        if not value:
            return ""
        
        escape_map = {
            self.encoding.field_separator: f"{self.encoding.escape_character}F{self.encoding.escape_character}",
            self.encoding.component_separator: f"{self.encoding.escape_character}S{self.encoding.escape_character}",
            self.encoding.repetition_separator: f"{self.encoding.escape_character}R{self.encoding.escape_character}",
            self.encoding.escape_character: f"{self.encoding.escape_character}E{self.encoding.escape_character}",
            self.encoding.subcomponent_separator: f"{self.encoding.escape_character}T{self.encoding.escape_character}",
        }
        
        result = value
        for char, escape_seq in escape_map.items():
            result = result.replace(char, escape_seq)
        
        return result
    
    def build(self) -> str:
        field_str = self.encoding.field_separator.join(self.fields)
        return f"{self.segment_id}{self.encoding.field_separator}{field_str}"

class HL7Builder:
    def __init__(self, config: HL7Config):
        self.config = config
        self.encoding = HL7EncodingCharacters()
        self.segments: List[HL7Segment] = []
        self.message_control_id = config.message_control_id or self._generate_control_id()
    
    def _generate_control_id(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
        return f"MSG{timestamp}"
    
    def _format_hl7_date(self, dt: Union[datetime, date]) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%Y%m%d%H%M%S")
        elif isinstance(dt, date):
            return dt.strftime("%Y%m%d")
        return ""
    
    def _format_name(self, name: str) -> str:
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[-1]}^{parts[0]}^{'^'.join(parts[1:-1])}"
        return name
    
    def add_msh_segment(self) -> None:
        msh = HL7Segment("MSH", self.encoding)
        
        msh.add_field(str(self.encoding), 2)
        msh.add_field(self.config.sending_application, 3)
        msh.add_field(self.config.sending_facility, 4)
        msh.add_field(self.config.receiving_application, 5)
        msh.add_field(self.config.receiving_facility, 6)
        msh.add_field(self._format_hl7_date(datetime.now()), 7)
        msh.add_field("", 8)
        msh.add_field(self.config.message_type.value, 9)
        msh.add_field(self.message_control_id, 10)
        msh.add_field(self.config.processing_id, 11)
        msh.add_field(self.config.version, 12)
        msh.add_field("", 13)
        msh.add_field("", 14)
        msh.add_field("AL", 15)
        msh.add_field("AL", 16)
        msh.add_field(self.config.country_code, 17)
        msh.add_field(self.config.charset, 18)
        msh.add_field("", 19)
        msh.add_field("", 20)
        
        self.segments.append(msh)
    
    def add_pid_segment(self, patient: PatientInfo) -> None:
        pid = HL7Segment("PID", self.encoding)
        
        pid.add_field("1", 1)
        pid.add_field("", 2)
        pid.add_field(f"{patient.patient_id}^^{self.config.sending_facility}^MR", 3)
        pid.add_field("", 4)
        pid.add_field(self._format_name(patient.name), 5)
        pid.add_field("", 6)
        pid.add_field(self._format_hl7_date(patient.date_of_birth), 7)
        pid.add_field(patient.gender, 8)
        pid.add_field("", 9)
        pid.add_field("", 10)
        pid.add_field("", 11)
        pid.add_field("", 12)
        pid.add_field("", 13)
        pid.add_field("", 14)
        pid.add_field("", 15)
        pid.add_field("", 16)
        pid.add_field("", 17)
        pid.add_field("", 18)
        pid.add_field("", 19)
        pid.add_field("", 20)
        pid.add_field("", 21)
        pid.add_field("", 22)
        pid.add_field("", 23)
        pid.add_field("", 24)
        pid.add_field("", 25)
        pid.add_field("", 26)
        pid.add_field("", 27)
        pid.add_field("", 28)
        pid.add_field("", 29)
        pid.add_field("", 30)
        
        self.segments.append(pid)
        
        if patient.weight_kg:
            obx = HL7Segment("OBX", self.encoding)
            obx.add_field(str(len([s for s in self.segments if s.segment_id == "OBX"]) + 1), 1)
            obx.add_field("NM", 2)
            obx.add_field("3141-9^Body weight Measured^LN", 3)
            obx.add_field("", 4)
            obx.add_field(str(patient.weight_kg), 5)
            obx.add_field("kg", 6)
            obx.add_field("", 7)
            obx.add_field("", 8)
            obx.add_field("", 9)
            obx.add_field("", 10)
            obx.add_field("F", 11)
            obx.add_field("", 12)
            obx.add_field("", 13)
            obx.add_field(self._format_hl7_date(datetime.now()), 14)
            obx.add_field("", 15)
            obx.add_field("", 16)
            self.segments.append(obx)
        
        if patient.height_cm:
            obx = HL7Segment("OBX", self.encoding)
            obx.add_field(str(len([s for s in self.segments if s.segment_id == "OBX"]) + 1), 1)
            obx.add_field("NM", 2)
            obx.add_field("8302-2^Body height^LN", 3)
            obx.add_field("", 4)
            obx.add_field(str(patient.height_cm), 5)
            obx.add_field("cm", 6)
            obx.add_field("", 7)
            obx.add_field("", 8)
            obx.add_field("", 9)
            obx.add_field("", 10)
            obx.add_field("F", 11)
            obx.add_field("", 12)
            obx.add_field("", 13)
            obx.add_field(self._format_hl7_date(datetime.now()), 14)
            obx.add_field("", 15)
            obx.add_field("", 16)
            self.segments.append(obx)
    
    def add_pv1_segment(self, patient_class: str = "O") -> None:
        pv1 = HL7Segment("PV1", self.encoding)
        
        pv1.add_field("1", 1)
        pv1.add_field(patient_class, 2)
        
        for i in range(3, 51):
            pv1.add_field("", i)
        
        self.segments.append(pv1)
    
    def add_orc_segment(
        self,
        order_control: str = "NW",
        placer_order_number: str = "",
        filler_order_number: str = "",
        order_status: str = "SC",
        response_flag: str = "",
        timing_quantity: List[str] = None,
        parent_order: str = "",
        datetime_of_transaction: Optional[datetime] = None,
        entered_by: Optional[PrescribingProvider] = None,
        verified_by: Optional[PrescribingProvider] = None,
        ordering_provider: Optional[PrescribingProvider] = None
    ) -> None:
        orc = HL7Segment("ORC", self.encoding)
        
        orc.add_field(order_control, 1)
        orc.add_field(placer_order_number, 2)
        orc.add_field(filler_order_number, 3)
        orc.add_field("", 4)
        orc.add_field(order_status, 5)
        orc.add_field(response_flag, 6)
        
        if timing_quantity:
            orc.add_field(self.encoding.component_separator.join(timing_quantity), 7)
        
        orc.add_field(parent_order, 8)
        orc.add_field(
            self._format_hl7_date(datetime_of_transaction) if datetime_of_transaction else 
            self._format_hl7_date(datetime.now()), 
            9
        )
        
        if entered_by:
            orc.add_field(f"{entered_by.name}^{entered_by.id}", 10)
        
        if verified_by:
            orc.add_field(f"{verified_by.name}^{verified_by.id}", 11)
        
        if ordering_provider:
            orc.add_field(f"{ordering_provider.name}^{ordering_provider.id}", 12)
        
        orc.add_field("", 13)
        orc.add_field("", 14)
        orc.add_field("", 15)
        orc.add_field("", 16)
        
        self.segments.append(orc)
    
    def add_rxe_segment(
        self,
        medication: MedicationItem,
        give_per: str = "DOSE",
        give_rate: Optional[str] = None,
        give_units: Optional[str] = None,
        give_strength: Optional[str] = None,
        give_strength_units: Optional[str] = None,
        provider_administration_instructions: Optional[str] = None,
        delivery_administration_instructions: Optional[str] = None
    ) -> None:
        rxe = HL7Segment("RXE", self.encoding)
        
        timing = []
        if medication.frequency:
            timing.append(medication.frequency)
        if medication.start_datetime:
            timing.append(self._format_hl7_date(medication.start_datetime))
        if medication.duration_days:
            timing.append(str(medication.duration_days))
            timing.append("D")
        
        rxe.add_field(self.encoding.component_separator.join(timing) if timing else "", 1)
        rxe.add_field(f"{medication.medication_code}^{medication.medication_name}^NDC", 2)
        rxe.add_field(str(medication.quantity), 3)
        rxe.add_field("", 4)
        rxe.add_field(medication.unit, 5)
        rxe.add_field(HL7_TABLES["medication_form"].get(medication.form, medication.form), 6)
        
        admin_instructions = medication.dosage_instruction
        if medication.special_instructions:
            admin_instructions += f"; {medication.special_instructions}"
        rxe.add_field(admin_instructions, 7)
        
        rxe.add_field("", 8)
        rxe.add_field("G" if medication.substitution_allowed else "N", 9)
        rxe.add_field(str(medication.quantity), 10)
        rxe.add_field(medication.unit, 11)
        rxe.add_field(str(medication.refills) if medication.refills else "0", 12)
        rxe.add_field("", 13)
        rxe.add_field("", 14)
        rxe.add_field("", 15)
        rxe.add_field(str(medication.refills) if medication.refills else "0", 16)
        rxe.add_field("0", 17)
        rxe.add_field("", 18)
        rxe.add_field("", 19)
        rxe.add_field("", 20)
        rxe.add_field("", 21)
        rxe.add_field(give_per, 22)
        rxe.add_field(give_rate, 23)
        rxe.add_field(give_units, 24)
        rxe.add_field(give_strength, 25)
        rxe.add_field(give_strength_units, 26)
        rxe.add_field("", 27)
        rxe.add_field("", 28)
        rxe.add_field("", 29)
        rxe.add_field("", 30)
        
        self.segments.append(rxe)
        self.add_rxr_segment(medication.route)
    
    def add_rxr_segment(self, route: str, site: Optional[str] = None) -> None:
        rxr = HL7Segment("RXR", self.encoding)
        
        rxr.add_field(f"{route}^{HL7_TABLES['route'].get(route, route)}^HL70162", 1)
        
        if site:
            rxr.add_field(site, 2)
        
        rxr.add_field("", 3)
        rxr.add_field("", 4)
        rxr.add_field("", 5)
        rxr.add_field("", 6)
        
        self.segments.append(rxr)
    
    def add_rxd_segment(
        self,
        medication: MedicationItem,
        dispense_number: int = 1,
        quantity_dispensed: Optional[Decimal] = None,
        fill_datetime: Optional[datetime] = None,
        days_supply: Optional[int] = None
    ) -> None:
        rxd = HL7Segment("RXD", self.encoding)
        
        rxd.add_field(str(dispense_number), 1)
        rxd.add_field(f"{medication.medication_code}^{medication.medication_name}^NDC", 2)
        rxd.add_field(
            self._format_hl7_date(fill_datetime) if fill_datetime else 
            self._format_hl7_date(datetime.now()), 
            3
        )
        rxd.add_field(str(quantity_dispensed if quantity_dispensed else medication.quantity), 4)
        rxd.add_field(medication.unit, 5)
        rxd.add_field(HL7_TABLES["medication_form"].get(medication.form, medication.form), 6)
        rxd.add_field("", 7)
        rxd.add_field(str(medication.refills) if medication.refills else "0", 8)
        rxd.add_field("", 9)
        rxd.add_field("", 10)
        rxd.add_field("G" if medication.substitution_allowed else "N", 11)
        rxd.add_field("", 12)
        rxd.add_field("", 13)
        rxd.add_field("", 14)
        rxd.add_field("", 15)
        rxd.add_field(medication.strength, 16)
        rxd.add_field("", 17)
        rxd.add_field("", 18)
        rxd.add_field("", 19)
        rxd.add_field("", 20)
        rxd.add_field("", 21)
        rxd.add_field("", 22)
        rxd.add_field("", 23)
        rxd.add_field("", 24)
        rxd.add_field("", 25)
        rxd.add_field("", 26)
        rxd.add_field("", 27)
        rxd.add_field("", 28)
        rxd.add_field("", 29)
        rxd.add_field("", 30)
        rxd.add_field("", 31)
        rxd.add_field("", 32)
        rxd.add_field("", 33)
        rxd.add_field("", 34)
        rxd.add_field("", 35)
        rxd.add_field("", 36)
        rxd.add_field("", 37)
        rxd.add_field("", 38)
        
        self.segments.append(rxd)
    
    def add_diagnosis_segments(self, diagnoses: List[Tuple[str, str]]) -> None:
        for idx, (code, description) in enumerate(diagnoses, 1):
            dg1 = HL7Segment("DG1", self.encoding)
            dg1.add_field(str(idx), 1)
            dg1.add_field("I10", 2)
            dg1.add_field(f"{code}^{description}^I10", 3)
            dg1.add_field("", 4)
            dg1.add_field(self._format_hl7_date(datetime.now()), 5)
            dg1.add_field("W", 6)
            dg1.add_field("", 7)
            dg1.add_field("", 8)
            dg1.add_field("", 9)
            dg1.add_field("", 10)
            dg1.add_field("", 11)
            dg1.add_field("", 12)
            dg1.add_field("", 13)
            dg1.add_field("", 14)
            dg1.add_field("", 15)
            dg1.add_field("", 16)
            dg1.add_field("", 17)
            dg1.add_field("", 18)
            dg1.add_field("", 19)
            dg1.add_field("", 20)
            dg1.add_field("", 21)
            self.segments.append(dg1)
    
    def add_allergy_segments(self, allergies: List[str]) -> None:
        for idx, allergy in enumerate(allergies, 1):
            al1 = HL7Segment("AL1", self.encoding)
            al1.add_field(str(idx), 1)
            al1.add_field("DA", 2)
            al1.add_field(allergy, 3)
            al1.add_field("", 4)
            al1.add_field("", 5)
            al1.add_field("", 6)
            self.segments.append(al1)
    
    def add_nte_segment(self, comment: str, set_id: int = 1, source: str = "P") -> None:
        nte = HL7Segment("NTE", self.encoding)
        nte.add_field(str(set_id), 1)
        nte.add_field(source, 2)
        nte.add_field(comment, 3)
        self.segments.append(nte)
    
    def build_message(self) -> str:
        if self.config.include_msh:
            if not any(s.segment_id == "MSH" for s in self.segments):
                self.add_msh_segment()
        
        segments_str = [segment.build() for segment in self.segments]
        return SEGMENT_DELIMITER.join(segments_str)

def convert_edifact_to_hl7(edifact_data: Dict[str, Any]) -> Dict[str, Any]:
    hl7_data = {
        "patient": PatientInfo(
            patient_id=edifact_data["patient"]["patient_id"],
            name=edifact_data["patient"]["name"],
            date_of_birth=datetime.strptime(
                edifact_data["patient"]["date_of_birth"], 
                "%Y%m%d"
            ).date(),
            gender=edifact_data["patient"]["gender"],
            weight_kg=Decimal(str(edifact_data["patient"].get("weight_kg", 0))) 
                     if edifact_data["patient"].get("weight_kg") else None,
            height_cm=Decimal(str(edifact_data["patient"].get("height_cm", 0))) 
                     if edifact_data["patient"].get("height_cm") else None,
            allergies=edifact_data["patient"].get("allergies", []),
            diagnoses=[(d, "") for d in edifact_data["patient"].get("diagnoses", [])]
        ),
        "provider": PrescribingProvider(
            id=edifact_data["prescribing_doctor"]["id"],
            name=edifact_data["prescribing_doctor"]["name"],
            qualification=edifact_data["prescribing_doctor"].get("qualification"),
            specialty=edifact_data["prescribing_doctor"].get("specialty"),
            contact=edifact_data["prescribing_doctor"].get("contact"),
            address=edifact_data["prescribing_doctor"].get("address")
        ),
        "pharmacy": PharmacyInfo(
            id=edifact_data["pharmacy"]["id"],
            name=edifact_data["pharmacy"]["name"],
            address=edifact_data["pharmacy"].get("address"),
            contact=edifact_data["pharmacy"].get("contact")
        ),
        "medications": [
            MedicationItem(
                medication_code=item["medication_code"],
                medication_name=item["medication_name"],
                form=item["form"],
                strength=item["strength"],
                quantity=Decimal(str(item["quantity"])),
                unit=item.get("unit", item["form"]),
                dosage_instruction=item["dosage_instruction"],
                route=item["route"],
                duration_days=item.get("duration_days"),
                refills=item.get("refills"),
                special_instructions=item.get("special_instructions"),
                substitution_allowed=item.get("substitution_allowed", True),
                frequency="QD"
            )
            for item in edifact_data["items"]
        ],
        "prescription_info": {
            "prescription_id": edifact_data["prescription_id"],
            "prescription_date": datetime.strptime(
                edifact_data["prescription_date"], 
                "%Y%m%d"
            ),
            "urgent": edifact_data.get("urgent", False),
            "validity_days": edifact_data.get("validity_days"),
            "payment_type": edifact_data.get("payment_type"),
            "insurance_info": edifact_data.get("insurance_info"),
            "clinical_notes": edifact_data.get("clinical_notes"),
            "dispense_as_written": not edifact_data.get("substitution_allowed", True)
        }
    }
    
    return hl7_data

def create_hl7_prescription(
    hl7_data: Dict[str, Any],
    config: Optional[HL7Config] = None
) -> str:
    if config is None:
        config = HL7Config()
    
    builder = HL7Builder(config)
    
    builder.add_msh_segment()
    builder.add_pid_segment(hl7_data["patient"])
    builder.add_pv1_segment(patient_class="O")
    
    builder.add_orc_segment(
        order_control="NW",
        placer_order_number=hl7_data["prescription_info"]["prescription_id"],
        order_status="SC",
        ordering_provider=hl7_data["provider"],
        datetime_of_transaction=hl7_data["prescription_info"]["prescription_date"]
    )
    
    if hl7_data["patient"].diagnoses:
        builder.add_diagnosis_segments(hl7_data["patient"].diagnoses)
    
    if hl7_data["patient"].allergies:
        builder.add_allergy_segments(hl7_data["patient"].allergies)
    
    if hl7_data["prescription_info"].get("clinical_notes"):
        builder.add_nte_segment(
            comment=hl7_data["prescription_info"]["clinical_notes"],
            source="P"
        )
    
    for idx, medication in enumerate(hl7_data["medications"], 1):
        builder.add_rxe_segment(medication)
        
        if config.message_type == MessageType.RDE:
            builder.add_rxd_segment(medication, dispense_number=idx)
    
    return builder.build_message()

def parse_hl7_response(hl7_message: str) -> Dict[str, Any]:
    lines = hl7_message.split(SEGMENT_DELIMITER)
    result = {
        "segments": [],
        "acknowledgment": None,
        "status": "unknown"
    }
    
    for line in lines:
        if line.startswith("MSH"):
            result["message_type"] = line.split("|")[8] if len(line.split("|")) > 8 else ""
            result["message_control_id"] = line.split("|")[9] if len(line.split("|")) > 9 else ""
        elif line.startswith("MSA"):
            parts = line.split("|")
            if len(parts) >= 2:
                result["acknowledgment"] = {
                    "code": parts[1],
                    "message": parts[2] if len(parts) > 2 else "",
                    "control_id": parts[3] if len(parts) > 3 else ""
                }
                if parts[1] == "AA":
                    result["status"] = "accepted"
                elif parts[1] == "AE":
                    result["status"] = "error"
                elif parts[1] == "AR":
                    result["status"] = "rejected"
        
        result["segments"].append(line[:3])
    
    return result

def main():
    edifact_prescription = {
        "message_ref": "MED0001",
        "prescription_id": "RX2025-0509-001",
        "prescription_date": "20241210",
        "urgent": False,
        "validity_days": 30,
        "payment_type": "INSURANCE",
        "insurance_info": {
            "id": "INS123456789",
            "name": "HealthCare Plus"
        },
        "dispense_as_written": False,
        "clinical_notes": "Patient has history of mild hypertension. Monitor blood pressure during treatment.",
        "prescribing_doctor": {
            "id": "DOC987654321",
            "name": "Dr. Jane Smith",
            "qualification": "MD",
            "specialty": "Internal Medicine",
            "contact": "+1-555-123-4567",
            "address": "123 Medical Center, Suite 100"
        },
        "patient": {
            "patient_id": "PAT123456789",
            "name": "John Doe",
            "date_of_birth": "19800515",
            "gender": "M",
            "weight_kg": "85.5",
            "height_cm": "180.0",
            "allergies": ["Penicillin", "Sulfa drugs"],
            "diagnoses": ["I10", "E11.9"]
        },
        "pharmacy": {
            "id": "PHARM12345",
            "name": "City Pharmacy",
            "address": "456 Main Street",
            "contact": "+1-555-987-6543"
        },
        "items": [
            {
                "medication_code": "C09AA01",
                "medication_name": "Lisinopril",
                "form": "TAB",
                "strength": "10 mg",
                "quantity": "30",
                "unit": "TAB",
                "dosage_instruction": "Take 1 tablet once daily in the morning",
                "route": "PO",
                "duration_days": 30,
                "refills": 3,
                "special_instructions": "Take with food if stomach upset occurs",
                "substitution_allowed": True
            }
        ]
    }
    
    try:
        logger.info("Converting EDIFACT to HL7 format...")
        hl7_data = convert_edifact_to_hl7(edifact_prescription)
        
        config = HL7Config(
            sending_application="EDIFACT_CONVERTER",
            sending_facility="HOSPITAL_XYZ",
            receiving_application="PHARMACY_SYSTEM",
            receiving_facility="PHARMACY_ABC",
            version="2.5",
            message_type=MessageType.RDE,
            processing_id="P"
        )
        
        logger.info("Generating HL7 message...")
        hl7_message = create_hl7_prescription(hl7_data, config)
        
        print("\n" + "="*80)
        print("HL7 PRESCRIPTION MESSAGE (RDE^O11)")
        print("="*80)
        print(hl7_message)
        
        with open("prescription.hl7", "w") as f:
            f.write(hl7_message)
        
        logger.info(f"HL7 message saved to prescription.hl7")
        
        segments = hl7_message.split(SEGMENT_DELIMITER)
        print(f"\nTotal segments: {len(segments)}")
        print("Segment types:", ", ".join(sorted(set(s[:3] for s in segments if s))))
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
