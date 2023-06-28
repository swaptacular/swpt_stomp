from marshmallow import (
    Schema, fields, validate, validates, validates_schema, ValidationError,
    EXCLUDE,
)

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
CONFIG_DATA_MAX_BYTES = 2000
COORDINATOR_TYPE_MAX_BYTES = 30
ACCOUNT_ID_MAX_BYTES = 100
TRANSFER_NOTE_MAX_BYTES = 500
TRANSFER_NOTE_FORMAT_REGEX = r'^[0-9A-Za-z.-]{0,8}$'
REJECTION_CODE_MAX_BYTES = 30
STATUS_CODE_MAX_BYTES = 30
IRI_MAX_LENGTH = 200
CONTENT_TYPE_MAX_BYTES = 100
DEBTOR_INFO_SHA256_REGEX = r'^([0-9A-F]{64}|[0-9a-f]{64})?$'
ROOT_CREDITOR_ID = 0

IN_MESSAGE_TYPES: set[str] = set([
    'ConfigureAccount',
    'PrepareTransfer',
    'FinalizeTransfer',
])

OUT_MESSAGE_TYPES: set[str] = set([
    'RejectedConfig',
    'RejectedTransfer',
    'PreparedTransfer',
    'FinalizedTransfer',
    'AccountUpdate',
    'AccountPurge',
    'AccountTransfer',
])


class _ValidateMixin:
    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))

    @validates('type')
    def validate_type(self, value):
        if f'{value}MessageSchema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class _ValidateCoordinatorTypeMixin(_ValidateMixin):
    coordinator_type = fields.String(
        required=True,
        validate=validate.Length(min=1, max=COORDINATOR_TYPE_MAX_BYTES),
    )

    @validates('coordinator_type')
    def validate_coordinator_type(self, value):
        if not value.isascii():
            raise ValidationError(
                'The coordinator_type field contains non-ASCII characters.')


class _ValidateCoordinatorFieldsMixin(_ValidateCoordinatorTypeMixin):
    coordinator_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    coordinator_request_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))


class _ValidateTransferFieldsMixin:
    transfer_note_format = fields.String(
        required=True, validate=validate.Regexp(TRANSFER_NOTE_FORMAT_REGEX))
    transfer_note = fields.String(
        required=True, validate=validate.Length(max=TRANSFER_NOTE_MAX_BYTES))

    @validates('transfer_note')
    def validate_transfer_note(self, value):
        if len(value.encode('utf8')) > TRANSFER_NOTE_MAX_BYTES:
            raise ValidationError('The length of transfer_note exceeds '
                                  f'{TRANSFER_NOTE_MAX_BYTES} bytes.')


class ConfigureAccountMessageSchema(  # type: ignore
        _ValidateMixin, Schema):
    """`ConfigureAccount` message schema.
    """
    negligible_amount = fields.Float(
        required=True, validate=validate.Range(min=0.0))
    config_data = fields.String(
        required=True, validate=validate.Length(max=CONFIG_DATA_MAX_BYTES))
    config_flags = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    seqnum = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    ts = fields.DateTime(required=True)

    @validates('config_data')
    def validate_config_data(self, value):
        if len(value.encode('utf8')) > CONFIG_DATA_MAX_BYTES:
            raise ValidationError('The length of config_data exceeds '
                                  f'{CONFIG_DATA_MAX_BYTES} bytes.')


class RejectedConfigMessageSchema(  # type: ignore
        _ValidateMixin, Schema):
    """`RejectedConfig` message schema.
    """
    config_ts = fields.DateTime(required=True)
    config_seqnum = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    config_flags = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    config_data = fields.String(
        required=True, validate=validate.Length(max=CONFIG_DATA_MAX_BYTES))
    negligible_amount = fields.Float(
        required=True, validate=validate.Range(min=0.0))
    rejection_code = fields.String(
        required=True, validate=validate.Length(max=REJECTION_CODE_MAX_BYTES))
    ts = fields.DateTime(required=True)

    @validates('config_data')
    def validate_config_data(self, value):
        if len(value.encode('utf8')) > CONFIG_DATA_MAX_BYTES:
            raise ValidationError('The length of config_data exceeds '
                                  f'{CONFIG_DATA_MAX_BYTES} bytes.')

    @validates('rejection_code')
    def validate_rejection_code(self, value):
        if not value.isascii():
            raise ValidationError(
                'The rejection_code field contains non-ASCII characters.')


class PrepareTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorFieldsMixin, Schema):
    """`PrepareTransfer` message schema.
    """
    min_locked_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    max_locked_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    recipient = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES))
    min_interest_rate = fields.Float(
        required=True, validate=validate.Range(min=-100.0))
    max_commit_delay = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT32))
    ts = fields.DateTime(required=True)

    @validates_schema
    def validate_max_locked_amount(self, data, **kwargs):
        if data['min_locked_amount'] > data['max_locked_amount']:
            raise ValidationError("max_locked_amount must be equal or "
                                  "greater than min_locked_amount.")

    @validates_schema
    def validate_coordinator(self, data, **kwargs):
        if (data['coordinator_type'] == 'direct'
                and data['coordinator_id'] != data['creditor_id']):
            raise ValidationError(
                "Invalid coordinator_id for direct transfer.")
        if (data['coordinator_type'] == 'issuing'
                and data['coordinator_id'] != data['debtor_id']):
            raise ValidationError(
                "Invalid coordinator_id for issuing transfer.")
        if (data['coordinator_type'] == 'issuing'
                and data['creditor_id'] != ROOT_CREDITOR_ID):
            raise ValidationError(
                "Invalid sender creditor_id for issuing transfer.")

    @validates('recipient')
    def validate_recipient(self, value):
        if not value.isascii():
            raise ValidationError(
                'The recipient field contains non-ASCII characters.')


class FinalizeTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorFieldsMixin, _ValidateTransferFieldsMixin, Schema):
    """`FinalizeTransfer` message schema.
    """
    transfer_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    committed_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    ts = fields.DateTime(required=True)


class RejectedTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorFieldsMixin, Schema):
    """`RejectedTransfer` message schema.
    """
    status_code = fields.String(
        required=True, validate=validate.Length(max=STATUS_CODE_MAX_BYTES))
    total_locked_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    ts = fields.DateTime(required=True)

    @validates('status_code')
    def validate_status_code(self, value):
        if value == 'OK':
            raise ValidationError('The status_code field contains '
                                  f'an invalid value ("{value}").')
        if not value.isascii():
            raise ValidationError(
                'The status_code field contains non-ASCII characters.')


class PreparedTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorFieldsMixin, Schema):
    """`PreparedTransfer` message schema.
    """
    transfer_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    locked_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    recipient = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES))
    prepared_at = fields.DateTime(required=True)
    demurrage_rate = fields.Float(
        required=True, validate=validate.Range(min=-100.0, max=0.0))
    deadline = fields.DateTime(required=True)
    min_interest_rate = fields.Float(
        required=True, validate=validate.Range(min=-100.0))
    ts = fields.DateTime(required=True)

    @validates('recipient')
    def validate_recipient(self, value):
        if not value.isascii():
            raise ValidationError(
                'The recipient field contains non-ASCII characters.')


class FinalizedTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorFieldsMixin, Schema):
    """`FinalizedTransfer` message schema.
    """
    transfer_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    committed_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    status_code = fields.String(
        required=True, validate=validate.Length(max=STATUS_CODE_MAX_BYTES))
    total_locked_amount = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    prepared_at = fields.DateTime(required=True)
    ts = fields.DateTime(required=True)

    @validates('status_code')
    def validate_status_code(self, value):
        if not value.isascii():
            raise ValidationError(
                'The status_code field contains non-ASCII characters.')

    @validates_schema
    def validate_committed_amount(self, data, **kwargs):
        if data['status_code'] != 'OK' and data['committed_amount'] != 0:
            raise ValidationError('The committed_amount must be zero '
                                  'when status_code is not "OK".')


class AccountUpdateMessageSchema(  # type: ignore
        _ValidateMixin, Schema):
    """`AccountUpdate` message schema.
    """
    creation_date = fields.Date(required=True)
    last_change_ts = fields.DateTime(required=True)
    last_change_seqnum = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    principal = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    interest = fields.Float(required=True)
    interest_rate = fields.Float(
        required=True, validate=validate.Range(min=-100.0))
    last_interest_rate_change_ts = fields.DateTime(required=True)
    last_config_ts = fields.DateTime(required=True)
    last_config_seqnum = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    negligible_amount = fields.Float(
        required=True, validate=validate.Range(min=0.0))
    config_flags = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32))
    config_data = fields.String(
        required=True, validate=validate.Length(max=CONFIG_DATA_MAX_BYTES))
    account_id = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES))
    debtor_info_iri = fields.String(
        required=True, validate=validate.Length(max=IRI_MAX_LENGTH))
    debtor_info_content_type = fields.String(
        required=True, validate=validate.Length(max=CONTENT_TYPE_MAX_BYTES))
    debtor_info_sha256 = fields.String(
        required=True, validate=validate.Regexp(DEBTOR_INFO_SHA256_REGEX))
    last_transfer_number = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))
    last_transfer_committed_at = fields.DateTime(required=True)
    demurrage_rate = fields.Float(
        required=True, validate=validate.Range(min=-100.0, max=0.0))
    commit_period = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT32))
    transfer_note_max_bytes = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=TRANSFER_NOTE_MAX_BYTES),
    )
    ts = fields.DateTime(required=True)
    ttl = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT32))

    @validates('config_data')
    def validate_config_data(self, value):
        if len(value.encode('utf8')) > CONFIG_DATA_MAX_BYTES:
            raise ValidationError('The length of config_data exceeds '
                                  f'{CONFIG_DATA_MAX_BYTES} bytes.')

    @validates('account_id')
    def validate_account_id(self, value):
        if not value.isascii():
            raise ValidationError(
                'The account_id field contains non-ASCII characters.')

    @validates('debtor_info_content_type')
    def validate_debtor_info_content_type(self, value):
        if not value.isascii():
            raise ValidationError('The debtor_info_content_type field '
                                  'contains non-ASCII characters.')


class AccountPurgeMessageSchema(  # type: ignore
        _ValidateMixin, Schema):
    """`AccountPurge` message schema.
    """
    creation_date = fields.Date(required=True)
    ts = fields.DateTime(required=True)


class AccountTransferMessageSchema(  # type: ignore
        _ValidateCoordinatorTypeMixin, _ValidateTransferFieldsMixin, Schema):
    """`AccountTransfer` message schema.
    """
    creation_date = fields.Date(required=True)
    transfer_number = fields.Integer(
        required=True, validate=validate.Range(min=1, max=MAX_INT64))
    sender = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES))
    recipient = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES))
    acquired_amount = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    committed_at = fields.DateTime(required=True)
    principal = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64))
    ts = fields.DateTime(required=True)
    previous_transfer_number = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64))

    @validates('sender')
    def validate_sender(self, value):
        if not value.isascii():
            raise ValidationError(
                'The sender field contains non-ASCII characters.')

    @validates('recipient')
    def validate_recipient(self, value):
        if not value.isascii():
            raise ValidationError(
                'The recipient field contains non-ASCII characters.')

    @validates('acquired_amount')
    def validate_acquired_amount(self, value):
        if value == 0:
            raise ValidationError(
                'The acquired_amount field is zero, which is not allowed.')

    @validates_schema
    def validate_transfer_number(self, data, **kwargs):
        if data['previous_transfer_number'] >= data['transfer_number']:
            raise ValidationError("transfer_number must be greater "
                                  "than previous_transfer_number.")


JSON_SCHEMAS: dict[str, Schema] = {
    'ConfigureAccount': ConfigureAccountMessageSchema(),
    'RejectedConfig': RejectedConfigMessageSchema(),
    'PrepareTransfer': PrepareTransferMessageSchema(),
    'FinalizeTransfer': FinalizeTransferMessageSchema(),
    'RejectedTransfer': RejectedTransferMessageSchema(),
    'PreparedTransfer': PreparedTransferMessageSchema(),
    'FinalizedTransfer': FinalizedTransferMessageSchema(),
    'AccountUpdate': AccountUpdateMessageSchema(),
    'AccountPurge': AccountPurgeMessageSchema(),
    'AccountTransfer': AccountTransferMessageSchema(),
}
