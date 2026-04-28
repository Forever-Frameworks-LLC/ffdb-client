// AUTO-GENERATED FILE - DO NOT EDIT

export interface User {
	id: string;
	name: string;
	email: string;
	emailVerified: number;
	image?: string;
	createdAt: string;
	updatedAt: string;
	role?: string;
	banned?: number;
	banReason?: string;
	banExpires?: string;
	twoFactorEnabled?: number;
	username?: string;
	displayUsername?: string;
}

export interface Session {
	id: string;
	expiresAt: string;
	token: string;
	createdAt: string;
	updatedAt: string;
	ipAddress?: string;
	userAgent?: string;
	userId: string;
	impersonatedBy?: string;
	activeOrganizationId?: string;
	activeTeamId?: string;
}

export interface Account {
	id: string;
	accountId: string;
	providerId: string;
	userId: string;
	accessToken?: string;
	refreshToken?: string;
	idToken?: string;
	accessTokenExpiresAt?: string;
	refreshTokenExpiresAt?: string;
	scope?: string;
	password?: string;
	createdAt: string;
	updatedAt: string;
}

export interface Verification {
	id: string;
	identifier: string;
	value: string;
	expiresAt: string;
	createdAt: string;
	updatedAt: string;
}

export interface Passkey {
	id: string;
	name?: string;
	publicKey: string;
	userId: string;
	credentialID: string;
	counter: number;
	deviceType: string;
	backedUp: number;
	transports?: string;
	createdAt?: string;
	aaguid?: string;
}

export interface TwoFactor {
	id: string;
	secret: string;
	backupCodes: string;
	userId: string;
	verified?: number;
}

export interface Apikey {
	id: string;
	configId: string;
	name?: string;
	start?: string;
	referenceId: string;
	prefix?: string;
	key: string;
	refillInterval?: number;
	refillAmount?: number;
	lastRefillAt?: string;
	enabled?: number;
	rateLimitEnabled?: number;
	rateLimitTimeWindow?: number;
	rateLimitMax?: number;
	requestCount?: number;
	remaining?: number;
	lastRequest?: string;
	expiresAt?: string;
	createdAt: string;
	updatedAt: string;
	permissions?: string;
	metadata?: string;
}

export interface Organization {
	id: string;
	name: string;
	slug: string;
	logo?: string;
	createdAt: string;
	metadata?: string;
}

export interface Team {
	id: string;
	name: string;
	organizationId: string;
	createdAt: string;
	updatedAt?: string;
}

export interface TeamMember {
	id: string;
	teamId: string;
	userId: string;
	createdAt?: string;
}

export interface Member {
	id: string;
	organizationId: string;
	userId: string;
	role: string;
	createdAt: string;
}

export interface Invitation {
	id: string;
	organizationId: string;
	email: string;
	role?: string;
	teamId?: string;
	status: string;
	expiresAt: string;
	createdAt: string;
	inviterId: string;
}

export interface DdlAudit {
	id: string;
	timestamp: number;
	user_id?: string;
	action: string;
	sql: string;
	snapshot_before?: string;
	snapshot_after?: string;
	success: number;
	error?: string;
}

export interface RowAudit {
	id: string;
	table_name: string;
	row_id?: string;
	action: string;
	before_json?: string;
	after_json?: string;
	timestamp: number;
}

export interface Policies {
	id: string;
	created_at: number;
	updated_at: number;
	user_id?: string;
	table_name: string;
	can_read?: number;
	can_insert?: number;
	can_update?: number;
	can_delete?: number;
	allowed_read_columns?: string;
	denied_read_columns?: string;
	writable_columns?: string;
	required_predicate_columns?: string;
	max_limit?: number;
	max_offset?: number;
	allow_offset?: number;
}

export interface TableCounts {
	table_name: string;
	row_count: number;
	updated_at: number;
}

export interface AdminChecklistSteps {
	user_id: string;
	step_id: string;
	completed: number;
	completed_at?: number;
	updated_at: number;
}

export interface Database {
	user: User;
	session: Session;
	account: Account;
	verification: Verification;
	passkey: Passkey;
	twoFactor: TwoFactor;
	apikey: Apikey;
	organization: Organization;
	team: Team;
	teamMember: TeamMember;
	member: Member;
	invitation: Invitation;
	ddl_audit: DdlAudit;
	row_audit: RowAudit;
	policies: Policies;
	table_counts: TableCounts;
	admin_checklist_steps: AdminChecklistSteps;
}
