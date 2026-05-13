import type { ClientRequestOptions } from "./client.ts";
import { compressImage, type CompressOptions } from "./image-compress.ts";

export type FileRecord = {
	id: string;
	name: string;
	s3_key: string;
	size_bytes: number;
	mime_type: string;
	folder_path: string;
	visibility: "public" | "private";
	privacy: "private" | "org" | "team";
	description: string | null;
	uploaded_by: string;
	org_id: string | null;
	team_id: string | null;
	status: "pending" | "confirmed" | "deleted";
	created_at: number;
	updated_at: number;
};

export type UploadOptions = {
	name?: string;
	folderPath?: string;
	visibility?: "public" | "private";
	privacy?: "private" | "org" | "team";
	description?: string;
	maxSizeBytes?: number;
	allowedMimeTypes?: string[];
	compress?: boolean;
	compressOptions?: CompressOptions;
};

export type FileUrl = {
	url: string;
	expiresAt: number | null;
	public: boolean;
};

export type FileListResult = {
	files: FileRecord[];
	total: number;
};

export type FilesNamespace = {
	upload: (
		file: File | Blob | { uri: string; type: string; name: string },
		opts?: UploadOptions,
	) => Promise<FileRecord>;
	list: (folder?: string) => Promise<FileListResult>;
	getUrl: (fileId: string) => Promise<FileUrl>;
	update: (
		fileId: string,
		metadata: Partial<
			Pick<FileRecord, "name" | "description" | "folder_path" | "visibility" | "privacy">
		>,
	) => Promise<FileRecord>;
	delete: (fileId: string) => Promise<{ deleted: boolean }>;
};

export class FileStorageError extends Error {
	code: string;
	constructor(message: string, code: string) {
		super(message);
		this.name = "FileStorageError";
		this.code = code;
	}
}

type ApiResponse<T> = { status: "ok"; data: T };

type ApiErrorResponse = {
	status: "error";
	error?: { message?: string; code?: string };
};

type RequestFn = <T = unknown>(
	url: string,
	options?: ClientRequestOptions,
) => Promise<T>;

function isReactNativeFile(
	input: unknown,
): input is { uri: string; type: string; name: string } {
	return (
		typeof input === "object" &&
		input !== null &&
		"uri" in input &&
		"type" in input &&
		"name" in input
	);
}

function isImageMime(mime: string): boolean {
	return mime.startsWith("image/");
}

async function resolveFileData(
	file: File | Blob | { uri: string; type: string; name: string },
): Promise<{ blob: Blob; name: string; type: string; size: number }> {
	if (isReactNativeFile(file)) {
		const response = await fetch(file.uri);
		const blob = await response.blob();
		return { blob, name: file.name, type: file.type, size: blob.size };
	}

	const name =
		file instanceof File ? file.name : `upload_${Date.now()}`;
	return { blob: file, name, type: file.type, size: file.size };
}

function isOfflineError(error: unknown): boolean {
	const message = error instanceof Error ? error.message : String(error ?? "");
	return /failed to fetch|network|err_connection_refused|load failed/i.test(message);
}

function wrapFileError(error: unknown): never {
	if (error instanceof FileStorageError) throw error;

	if (isOfflineError(error)) {
		throw new FileStorageError(
			"File operation failed: you appear to be offline. File uploads and downloads require a network connection.",
			"OFFLINE",
		);
	}

	const e = error as { status?: number; code?: string; message?: string };

	if (e?.code === "FILE_UPLOAD_DISABLED") {
		throw new FileStorageError(
			"File storage is not enabled. An admin must configure S3 credentials and enable file uploads in Settings.",
			"FILE_UPLOAD_DISABLED",
		);
	}

	if (e?.code === "S3_ERROR") {
		throw new FileStorageError(
			"S3 storage is not properly configured. Check your S3 endpoint, credentials, and bucket settings.",
			"S3_ERROR",
		);
	}

	if (e?.code === "FILE_TOO_LARGE") {
		throw new FileStorageError(
			e.message ?? "File exceeds the maximum allowed size.",
			"FILE_TOO_LARGE",
		);
	}

	if (e?.code === "MIME_TYPE_NOT_ALLOWED") {
		throw new FileStorageError(
			e.message ?? "This file type is not allowed.",
			"MIME_TYPE_NOT_ALLOWED",
		);
	}

	if (e?.code === "PERMISSION_DENIED") {
		throw new FileStorageError(
			e.message ?? "You do not have permission for this file operation.",
			"PERMISSION_DENIED",
		);
	}

	if (e?.status === 402) {
		throw new FileStorageError(
			e.message ?? "Storage limit exceeded. Upgrade your plan or free up space.",
			"STORAGE_LIMIT_EXCEEDED",
		);
	}

	throw error;
}

export function createFilesNamespace(request: RequestFn): FilesNamespace {
	async function upload(
		file: File | Blob | { uri: string; type: string; name: string },
		opts: UploadOptions = {},
	): Promise<FileRecord> {
		let { blob, name, type, size } = await resolveFileData(file);

		if (opts.name) name = opts.name;

		if (opts.compress && isImageMime(type)) {
			blob = await compressImage(blob, opts.compressOptions);
			size = blob.size;
		}

		let presignResponse: ApiResponse<{
			fileId: string;
			uploadUrl: string;
			s3Key: string;
			expiresAt: number;
		}>;

		try {
			presignResponse = await request<
				ApiResponse<{ fileId: string; uploadUrl: string; s3Key: string; expiresAt: number }>
			>("/api/files/upload", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					name,
					mimeType: type,
					sizeBytes: size,
					folderPath: opts.folderPath,
					visibility: opts.visibility,
					privacy: opts.privacy,
					description: opts.description,
					maxSizeOverride: opts.maxSizeBytes,
					allowedMimeOverride: opts.allowedMimeTypes,
				}),
			});
		} catch (error) {
			wrapFileError(error);
		}

		const { fileId, uploadUrl } = presignResponse.data;

		const uploadResponse = await fetch(uploadUrl, {
			method: "PUT",
			headers: { "Content-Type": type },
			body: blob,
		});

		if (!uploadResponse.ok) {
			throw new FileStorageError(
				`S3 upload failed with status ${uploadResponse.status}. The presigned URL may have expired — try again.`,
				"S3_UPLOAD_FAILED",
			);
		}

		try {
			const confirmResponse = await request<ApiResponse<FileRecord>>(
				`/api/files/${fileId}/confirm`,
				{ method: "POST" },
			);
			return confirmResponse.data;
		} catch (error) {
			wrapFileError(error);
		}
	}

	async function list(folder?: string): Promise<FileListResult> {
		try {
			const params = new URLSearchParams();
			if (folder) params.set("folder", folder);
			const url = `/api/files${params.toString() ? `?${params}` : ""}`;
			const response = await request<ApiResponse<FileListResult>>(url);
			return response.data;
		} catch (error) {
			wrapFileError(error);
		}
	}

	async function getUrl(fileId: string): Promise<FileUrl> {
		try {
			const response = await request<ApiResponse<FileUrl>>(
				`/api/files/${fileId}/url`,
			);
			return response.data;
		} catch (error) {
			wrapFileError(error);
		}
	}

	async function update(
		fileId: string,
		metadata: Partial<
			Pick<FileRecord, "name" | "description" | "folder_path" | "visibility" | "privacy">
		>,
	): Promise<FileRecord> {
		try {
			const response = await request<ApiResponse<FileRecord>>(
				`/api/files/${fileId}`,
				{
					method: "PATCH",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify(metadata),
				},
			);
			return response.data;
		} catch (error) {
			wrapFileError(error);
		}
	}

	async function del(fileId: string): Promise<{ deleted: boolean }> {
		try {
			const response = await request<ApiResponse<{ deleted: boolean }>>(
				`/api/files/${fileId}`,
				{ method: "DELETE" },
			);
			return response.data;
		} catch (error) {
			wrapFileError(error);
		}
	}

	return {
		upload,
		list,
		getUrl,
		update,
		delete: del,
	};
}
