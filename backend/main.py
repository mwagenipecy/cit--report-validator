from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from datetime import datetime, date
import re
from typing import Dict, List, Any, Optional
from pydantic import BaseModel
import openpyxl
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CIT Report Validator", version="1.0.0")

# Enhanced CORS middleware - more permissive for debugging
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",  # In case React runs on different port
        "http://127.0.0.1:3001"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Add middleware to handle OPTIONS requests explicitly
@app.middleware("http")
async def cors_handler(request: Request, call_next):
    if request.method == "OPTIONS":
        response = JSONResponse(content={})
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response
    
    response = await call_next(request)
    return response

# Add a logging middleware to debug requests
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    logger.info(f"Headers: {dict(request.headers)}")
    
    response = await call_next(request)
    
    logger.info(f"Response status: {response.status_code}")
    return response

# Response Models
class ValidationResult(BaseModel):
    field: str
    status: str  # 'pass', 'warning', 'fail'
    message: str
    details: Optional[List[str]] = None

class SheetValidation(BaseModel):
    status: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    validations: List[ValidationResult]

class ValidationResponse(BaseModel):
    overall_status: str
    total_records: int
    passed_records: int
    failed_records: int
    validation_timestamp: str
    sheets: Dict[str, SheetValidation]
    critical_issues: List[str]

# Test endpoints to verify CORS
@app.get("/test")
async def test_endpoint():
    """Simple test endpoint to verify server is running"""
    return {"message": "Server is running!", "timestamp": datetime.now().isoformat()}

@app.options("/validate")
async def validate_options():
    """Handle OPTIONS preflight request for /validate endpoint"""
    return JSONResponse(content={})

class CITValidator:
    """Main validator class for CIT reports"""
    
    def __init__(self):
        self.critical_issues = []
        self.validation_results = {}
        
    def validate_file(self, file_content: bytes) -> ValidationResponse:
        """Main validation method"""
        try:
            # Load Excel file
            workbook = openpyxl.load_workbook(BytesIO(file_content))
            
            # Expected sheets
            expected_sheets = ["Contract Data", "Subject Relation", "Company", "Individual"]
            
            # Check if all required sheets exist
            missing_sheets = [sheet for sheet in expected_sheets if sheet not in workbook.sheetnames]
            if missing_sheets:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Missing required sheets: {', '.join(missing_sheets)}"
                )
            
            # Validate each sheet
            sheet_results = {}
            total_records = 0
            passed_records = 0
            failed_records = 0
            
            for sheet_name in expected_sheets:
                sheet_data = pd.read_excel(BytesIO(file_content), sheet_name=sheet_name)
                validation_result = self._validate_sheet(sheet_name, sheet_data)
                sheet_results[sheet_name] = validation_result
                
                total_records += validation_result.total_rows
                passed_records += validation_result.passed_rows
                failed_records += validation_result.failed_rows
            
            # Determine overall status
            overall_status = self._determine_overall_status(sheet_results)
            
            return ValidationResponse(
                overall_status=overall_status,
                total_records=total_records,
                passed_records=passed_records,
                failed_records=failed_records,
                validation_timestamp=datetime.now().isoformat(),
                sheets=sheet_results,
                critical_issues=self.critical_issues
            )
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")
    
    def _validate_sheet(self, sheet_name: str, data: pd.DataFrame) -> SheetValidation:
        """Validate individual sheet"""
        if sheet_name == "Contract Data":
            return self._validate_contract_data(data)
        elif sheet_name == "Subject Relation":
            return self._validate_subject_relation(data)
        elif sheet_name == "Company":
            return self._validate_company_data(data)
        elif sheet_name == "Individual":
            return self._validate_individual_data(data)
        else:
            raise ValueError(f"Unknown sheet: {sheet_name}")
    
    def _validate_contract_data(self, data: pd.DataFrame) -> SheetValidation:
        """Validate Contract Data sheet"""
        validations = []
        failed_rows = 0
        total_rows = len(data)
        
        # Required columns
        required_columns = [
            "Reporting Date", "Contract code", "Customer Code", "Phase of Contract",
            "Type of Contract", "Purpose of Financing", "Total Amount", "Currency of Contract"
        ]
        
        # Check required columns exist
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            validations.append(ValidationResult(
                field="Required Columns",
                status="fail",
                message=f"Missing columns: {', '.join(missing_cols)}"
            ))
            self.critical_issues.append(f"Contract Data: Missing required columns: {', '.join(missing_cols)}")
        
        # Validate dates
        date_validation = self._validate_dates(data, "Reporting Date")
        validations.append(date_validation)
        if date_validation.status == "fail":
            failed_rows += len(date_validation.details) if date_validation.details else 0
        
        # Validate contract codes (should be unique)
        if "Contract code" in data.columns:
            duplicate_contracts = data[data["Contract code"].duplicated()]
            if not duplicate_contracts.empty:
                validations.append(ValidationResult(
                    field="Contract Code",
                    status="fail",
                    message=f"Found {len(duplicate_contracts)} duplicate contract codes"
                ))
                failed_rows += len(duplicate_contracts)
                self.critical_issues.append(f"Duplicate contract codes found: {len(duplicate_contracts)} records")
            else:
                validations.append(ValidationResult(
                    field="Contract Code",
                    status="pass",
                    message="All contract codes are unique"
                ))
        
        # Validate customer codes
        if "Customer Code" in data.columns:
            invalid_customer_codes = data[data["Customer Code"].isna() | (data["Customer Code"] == "")]
            if not invalid_customer_codes.empty:
                validations.append(ValidationResult(
                    field="Customer Code",
                    status="fail",
                    message=f"Found {len(invalid_customer_codes)} empty customer codes"
                ))
                failed_rows += len(invalid_customer_codes)
            else:
                validations.append(ValidationResult(
                    field="Customer Code",
                    status="pass",
                    message="All customer codes are valid"
                ))
        
        # Validate amounts
        amount_validation = self._validate_amounts(data, ["Total Amount", "Outstanding Amount", "Past Due Amount"])
        validations.append(amount_validation)
        if amount_validation.status == "fail":
            failed_rows += len(amount_validation.details) if amount_validation.details else 0
        
        # Validate currency
        if "Currency of Contract" in data.columns:
            valid_currencies = ["TZS", "USD", "EUR", "GBP"]
            invalid_currencies = data[~data["Currency of Contract"].isin(valid_currencies)]
            if not invalid_currencies.empty:
                validations.append(ValidationResult(
                    field="Currency",
                    status="warning",
                    message=f"Found {len(invalid_currencies)} records with non-standard currencies"
                ))
            else:
                validations.append(ValidationResult(
                    field="Currency",
                    status="pass",
                    message="All currencies are valid"
                ))
        
        passed_rows = total_rows - failed_rows
        status = "fail" if failed_rows > total_rows * 0.1 else "warning" if failed_rows > 0 else "pass"
        
        return SheetValidation(
            status=status,
            total_rows=total_rows,
            passed_rows=passed_rows,
            failed_rows=failed_rows,
            validations=validations
        )
    
    def _validate_subject_relation(self, data: pd.DataFrame) -> SheetValidation:
        """Validate Subject Relation sheet"""
        validations = []
        failed_rows = 0
        total_rows = len(data)
        
        # Validate National IDs (Tanzanian format)
        if "National ID" in data.columns:
            national_id_validation = self._validate_national_ids(data["National ID"])
            validations.append(national_id_validation)
            if national_id_validation.status == "fail":
                failed_rows += len(national_id_validation.details) if national_id_validation.details else 0
        
        # Validate phone numbers
        if "Phone" in data.columns:
            phone_validation = self._validate_phone_numbers(data["Phone"])
            validations.append(phone_validation)
        
        # Validate relation types
        if "Relation Type" in data.columns:
            valid_relations = ["Director", "Shareholder", "Guarantor", "Spouse", "Other"]
            invalid_relations = data[~data["Relation Type"].isin(valid_relations)]
            if not invalid_relations.empty:
                validations.append(ValidationResult(
                    field="Relation Type",
                    status="warning",
                    message=f"Found {len(invalid_relations)} records with non-standard relation types"
                ))
            else:
                validations.append(ValidationResult(
                    field="Relation Type",
                    status="pass",
                    message="All relation types are valid"
                ))
        
        passed_rows = total_rows - failed_rows
        status = "fail" if failed_rows > total_rows * 0.1 else "warning" if failed_rows > 0 else "pass"
        
        return SheetValidation(
            status=status,
            total_rows=total_rows,
            passed_rows=passed_rows,
            failed_rows=failed_rows,
            validations=validations
        )
    
    def _validate_company_data(self, data: pd.DataFrame) -> SheetValidation:
        """Validate Company sheet"""
        validations = []
        failed_rows = 0
        total_rows = len(data)
        
        # Validate registration numbers
        if "Registration Number" in data.columns:
            invalid_reg_numbers = data[data["Registration Number"].isna() | (data["Registration Number"] <= 0)]
            if not invalid_reg_numbers.empty:
                validations.append(ValidationResult(
                    field="Registration Number",
                    status="fail",
                    message=f"Found {len(invalid_reg_numbers)} invalid registration numbers"
                ))
                failed_rows += len(invalid_reg_numbers)
            else:
                validations.append(ValidationResult(
                    field="Registration Number",
                    status="pass",
                    message="All registration numbers are valid"
                ))
        
        # Validate Tax IDs (Tanzanian format: XXX-XXX-XXX)
        if "Tax Identification Number" in data.columns:
            tax_id_pattern = r'^\d{3}-\d{3}-\d{3}$'
            invalid_tax_ids = data[~data["Tax Identification Number"].astype(str).str.match(tax_id_pattern, na=False)]
            if not invalid_tax_ids.empty:
                validations.append(ValidationResult(
                    field="Tax ID Format",
                    status="warning",
                    message=f"Found {len(invalid_tax_ids)} records with non-standard tax ID format"
                ))
            else:
                validations.append(ValidationResult(
                    field="Tax ID Format",
                    status="pass",
                    message="All tax IDs follow the correct format"
                ))
        
        # Validate establishment dates
        if "Establishment Date" in data.columns:
            date_validation = self._validate_dates(data, "Establishment Date", future_allowed=False)
            validations.append(date_validation)
        
        passed_rows = total_rows - failed_rows
        status = "fail" if failed_rows > total_rows * 0.1 else "warning" if failed_rows > 0 else "pass"
        
        return SheetValidation(
            status=status,
            total_rows=total_rows,
            passed_rows=passed_rows,
            failed_rows=failed_rows,
            validations=validations
        )
    
    def _validate_individual_data(self, data: pd.DataFrame) -> SheetValidation:
        """Validate Individual sheet"""
        validations = []
        failed_rows = 0
        total_rows = len(data)
        
        # Validate birth dates
        if "Date of Birth" in data.columns:
            birth_date_validation = self._validate_birth_dates(data["Date of Birth"])
            validations.append(birth_date_validation)
            if birth_date_validation.status == "fail":
                failed_rows += len(birth_date_validation.details) if birth_date_validation.details else 0
        
        # Validate National IDs
        if "National ID" in data.columns:
            national_id_validation = self._validate_national_ids(data["National ID"])
            validations.append(national_id_validation)
            if national_id_validation.status == "fail":
                failed_rows += len(national_id_validation.details) if national_id_validation.details else 0
        
        # Validate gender
        if "Gender" in data.columns:
            valid_genders = ["Male", "Female", "Other"]
            invalid_genders = data[~data["Gender"].isin(valid_genders)]
            if not invalid_genders.empty:
                validations.append(ValidationResult(
                    field="Gender",
                    status="fail",
                    message=f"Found {len(invalid_genders)} records with invalid gender values"
                ))
                failed_rows += len(invalid_genders)
            else:
                validations.append(ValidationResult(
                    field="Gender",
                    status="pass",
                    message="All gender values are valid"
                ))
        
        passed_rows = total_rows - failed_rows
        status = "fail" if failed_rows > total_rows * 0.1 else "warning" if failed_rows > 0 else "pass"
        
        return SheetValidation(
            status=status,
            total_rows=total_rows,
            passed_rows=passed_rows,
            failed_rows=failed_rows,
            validations=validations
        )
    
    def _validate_dates(self, data: pd.DataFrame, column: str, future_allowed: bool = True) -> ValidationResult:
        """Validate date format and values"""
        if column not in data.columns:
            return ValidationResult(field=column, status="fail", message=f"Column {column} not found")
        
        try:
            # Convert to datetime
            dates = pd.to_datetime(data[column], errors='coerce')
            invalid_dates = dates.isna()
            
            details = []
            if invalid_dates.sum() > 0:
                details.append(f"{invalid_dates.sum()} records with invalid date format")
            
            if not future_allowed:
                future_dates = dates > pd.Timestamp.now()
                if future_dates.sum() > 0:
                    details.append(f"{future_dates.sum()} records with future dates")
                    if column == "Date of Birth":
                        self.critical_issues.append(f"{future_dates.sum()} individuals have future birth dates")
            
            if details:
                return ValidationResult(
                    field=column,
                    status="fail" if invalid_dates.sum() > len(data) * 0.1 else "warning",
                    message="; ".join(details),
                    details=details
                )
            else:
                return ValidationResult(
                    field=column,
                    status="pass",
                    message="All dates are valid"
                )
        except Exception as e:
            return ValidationResult(
                field=column,
                status="fail",
                message=f"Date validation error: {str(e)}"
            )
    
    def _validate_birth_dates(self, birth_dates: pd.Series) -> ValidationResult:
        """Validate birth dates specifically"""
        try:
            dates = pd.to_datetime(birth_dates, errors='coerce')
            
            issues = []
            invalid_count = 0
            
            # Check for invalid date formats
            invalid_dates = dates.isna()
            if invalid_dates.sum() > 0:
                issues.append(f"{invalid_dates.sum()} records with invalid date format")
                invalid_count += invalid_dates.sum()
            
            # Check for future dates
            future_dates = dates > pd.Timestamp.now()
            if future_dates.sum() > 0:
                issues.append(f"{future_dates.sum()} records with future birth dates")
                invalid_count += future_dates.sum()
                self.critical_issues.append(f"{future_dates.sum()} individuals have invalid birth dates (future dates)")
            
            # Check for unreasonable ages (over 120 years old or under 18 for business context)
            current_year = datetime.now().year
            ages = current_year - dates.dt.year
            too_old = ages > 120
            too_young = ages < 18
            
            if too_old.sum() > 0:
                issues.append(f"{too_old.sum()} records with unrealistic ages (over 120)")
                invalid_count += too_old.sum()
            
            if too_young.sum() > 0:
                issues.append(f"{too_young.sum()} records under 18 years old")
            
            if issues:
                status = "fail" if invalid_count > len(birth_dates) * 0.1 else "warning"
                return ValidationResult(
                    field="Date of Birth",
                    status=status,
                    message="; ".join(issues),
                    details=[str(invalid_count)]
                )
            else:
                return ValidationResult(
                    field="Date of Birth",
                    status="pass",
                    message="All birth dates are valid"
                )
                
        except Exception as e:
            return ValidationResult(
                field="Date of Birth",
                status="fail",
                message=f"Birth date validation error: {str(e)}"
            )
    
    def _validate_national_ids(self, national_ids: pd.Series) -> ValidationResult:
        """Validate Tanzanian National ID format and checksum"""
        try:
            # Tanzanian National ID format: YYYYMMDD-NNNNN-NNNNN-NN
            pattern = r'^\d{8}-\d{5}-\d{5}-\d{2}'
            
            issues = []
            invalid_count = 0
            
            # Remove NaN values for validation
            valid_ids = national_ids.dropna().astype(str)
            
            # Check format
            invalid_format = ~valid_ids.str.match(pattern)
            if invalid_format.sum() > 0:
                issues.append(f"{invalid_format.sum()} records with invalid ID format")
                invalid_count += invalid_format.sum()
            
            # Check birth date in ID matches reasonable range
            for idx, nid in valid_ids.items():
                if re.match(pattern, nid):
                    birth_part = nid[:8]
                    try:
                        birth_date = datetime.strptime(birth_part, '%Y%m%d')
                        age = (datetime.now() - birth_date).days / 365.25
                        if age < 0 or age > 120:
                            invalid_count += 1
                    except ValueError:
                        invalid_count += 1
            
            if invalid_count > 0:
                issues.append(f"{invalid_count} records with invalid birth dates in National ID")
                self.critical_issues.append(f"{invalid_count} National IDs fail validation checks")
            
            if issues:
                status = "fail" if invalid_count > len(valid_ids) * 0.1 else "warning"
                return ValidationResult(
                    field="National ID",
                    status=status,
                    message="; ".join(issues),
                    details=[str(invalid_count)]
                )
            else:
                return ValidationResult(
                    field="National ID",
                    status="pass",
                    message="All National IDs are valid"
                )
                
        except Exception as e:
            return ValidationResult(
                field="National ID",
                status="fail",
                message=f"National ID validation error: {str(e)}"
            )
    
    def _validate_phone_numbers(self, phone_numbers: pd.Series) -> ValidationResult:
        """Validate phone number format"""
        try:
            # Tanzanian phone number patterns
            patterns = [
                r'^(\+255|0)[67]\d{8}',  # Mobile numbers
                r'^(\+255|0)2[2-9]\d{7}'  # Landline numbers
            ]
            
            valid_phones = phone_numbers.dropna().astype(str)
            invalid_count = 0
            
            for phone in valid_phones:
                phone_clean = re.sub(r'[\s\-\(\)]', '', phone)
                if not any(re.match(pattern, phone_clean) for pattern in patterns):
                    invalid_count += 1
            
            if invalid_count > 0:
                return ValidationResult(
                    field="Phone Numbers",
                    status="warning",
                    message=f"{invalid_count} records with non-standard phone format"
                )
            else:
                return ValidationResult(
                    field="Phone Numbers",
                    status="pass",
                    message="All phone numbers are valid"
                )
                
        except Exception as e:
            return ValidationResult(
                field="Phone Numbers",
                status="fail",
                message=f"Phone validation error: {str(e)}"
            )
    
    def _validate_amounts(self, data: pd.DataFrame, amount_columns: List[str]) -> ValidationResult:
        """Validate monetary amounts"""
        try:
            issues = []
            
            for col in amount_columns:
                if col in data.columns:
                    amounts = pd.to_numeric(data[col], errors='coerce')
                    
                    # Check for negative amounts
                    negative_amounts = amounts < 0
                    if negative_amounts.sum() > 0:
                        issues.append(f"{col}: {negative_amounts.sum()} negative values")
                    
                    # Check for unreasonably large amounts (over 1 billion)
                    large_amounts = amounts > 1_000_000_000
                    if large_amounts.sum() > 0:
                        issues.append(f"{col}: {large_amounts.sum()} unusually large values")
                    
                    # Check for missing amounts in required fields
                    missing_amounts = amounts.isna()
                    if missing_amounts.sum() > 0:
                        issues.append(f"{col}: {missing_amounts.sum()} missing values")
            
            if issues:
                return ValidationResult(
                    field="Amount Fields",
                    status="warning" if len(issues) < 3 else "fail",
                    message="; ".join(issues)
                )
            else:
                return ValidationResult(
                    field="Amount Fields",
                    status="pass",
                    message="All amounts are valid"
                )
                
        except Exception as e:
            return ValidationResult(
                field="Amount Fields",
                status="fail",
                message=f"Amount validation error: {str(e)}"
            )
    
    def _determine_overall_status(self, sheet_results: Dict[str, SheetValidation]) -> str:
        """Determine overall validation status"""
        fail_count = sum(1 for result in sheet_results.values() if result.status == "fail")
        warning_count = sum(1 for result in sheet_results.values() if result.status == "warning")
        
        if fail_count > 0:
            return "fail"
        elif warning_count > 0:
            return "partial_pass"
        else:
            return "pass"

# API Endpoints
@app.post("/validate", response_model=ValidationResponse)
async def validate_file(file: UploadFile = File(...)):
    """Validate uploaded CIT report"""
    
    logger.info(f"Received file: {file.filename}")
    
    # Check file type
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls)"
        )
    
    # Read file content first to check size
    content = await file.read()
    file_size = len(content)
    
    # Check file size (max 10MB)
    if file_size > 10 * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail="File too large. Maximum size is 10MB"
        )
    
    try:
        # Initialize validator and run validation
        validator = CITValidator()
        result = validator.validate_file(content)
        
        logger.info(f"Validation completed for file: {file.filename}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error validating file {file.filename}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during validation: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "CIT Report Validator API",
        "version": "1.0.0",
        "endpoints": {
            "validate": "POST /validate - Upload and validate CIT report",
            "health": "GET /health - Health check",
            "test": "GET /test - Test endpoint"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)