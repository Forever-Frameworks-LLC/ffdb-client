import { useCallback, useContext, useEffect, useRef, useState } from "react";
import type {
	FileListResult,
	FileRecord,
	FileUrl,
	FilesNamespace,
	UploadOptions,
} from "../files.ts";
import { FileStorageError } from "../files.ts";
import { FFDBContext } from "./context.tsx";

// ── useFileUrl ────────────────────────────────────────────────────────
// Fetches a presigned download URL and automatically refreshes it before
// it expires, so components can bind the URL to <img src> or <a href>
// without worrying about expiry.

export type UseFileUrlOptions = {
	enabled?: boolean;
	/** Safety margin in ms before expiry to trigger a refresh (default: 60 000) */
	refreshBeforeExpiryMs?: number;
};

export type UseFileUrlResult = {
	url: string | null;
	isPublic: boolean;
	isLoading: boolean;
	error: Error | null;
	refetch: () => Promise<void>;
};

export function useFileUrl(
	fileId: string | null | undefined,
	options: UseFileUrlOptions = {},
): UseFileUrlResult {
	const { enabled = true, refreshBeforeExpiryMs = 60_000 } = options;
	const ctx = useContext(FFDBContext);
	const files = ctx?.client?.files as FilesNamespace | undefined;

	const [url, setUrl] = useState<string | null>(null);
	const [isPublic, setIsPublic] = useState(false);
	const [isLoading, setIsLoading] = useState(false);
	const [error, setError] = useState<Error | null>(null);

	const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
	const fetchIdRef = useRef(0);

	const clearTimer = useCallback(() => {
		if (timerRef.current) {
			clearTimeout(timerRef.current);
			timerRef.current = null;
		}
	}, []);

	const fetchUrl = useCallback(async () => {
		if (!files || !fileId || !enabled) return;

		const id = ++fetchIdRef.current;
		setIsLoading(true);
		clearTimer();

		try {
			const result = await files.getUrl(fileId);
			if (id !== fetchIdRef.current) return;

			setUrl(result.url);
			setIsPublic(result.public);
			setError(null);

			if (result.expiresAt && !result.public) {
				const msUntilExpiry = result.expiresAt - Date.now();
				const refreshIn = Math.max(1000, msUntilExpiry - refreshBeforeExpiryMs);
				timerRef.current = setTimeout(() => {
					void fetchUrl();
				}, refreshIn);
			}
		} catch (err) {
			if (id !== fetchIdRef.current) return;
			setError(err instanceof Error ? err : new Error(String(err)));
			setUrl(null);
		} finally {
			if (id === fetchIdRef.current) setIsLoading(false);
		}
	}, [files, fileId, enabled, refreshBeforeExpiryMs, clearTimer]);

	useEffect(() => {
		if (!fileId || !enabled) {
			setUrl(null);
			setError(null);
			setIsLoading(false);
			clearTimer();
			return;
		}
		void fetchUrl();
		return clearTimer;
	}, [fetchUrl, fileId, enabled, clearTimer]);

	return { url, isPublic, isLoading, error, refetch: fetchUrl };
}

// ── useFileList ───────────────────────────────────────────────────────
// Lists files in a folder with loading/error state and manual refetch.

export type UseFileListOptions = {
	enabled?: boolean;
	refetchOnFocus?: boolean;
	refetchInterval?: number;
};

export type UseFileListResult = {
	files: FileRecord[];
	total: number;
	isLoading: boolean;
	isFetching: boolean;
	error: Error | null;
	refetch: () => Promise<void>;
};

export function useFileList(
	folder?: string,
	options: UseFileListOptions = {},
): UseFileListResult {
	const { enabled = true, refetchOnFocus = true, refetchInterval } = options;
	const ctx = useContext(FFDBContext);
	const filesNs = ctx?.client?.files as FilesNamespace | undefined;

	const [data, setData] = useState<FileListResult>({ files: [], total: 0 });
	const [isLoading, setIsLoading] = useState(true);
	const [isFetching, setIsFetching] = useState(false);
	const [error, setError] = useState<Error | null>(null);
	const fetchIdRef = useRef(0);

	const execute = useCallback(async () => {
		if (!filesNs || !enabled) return;

		const id = ++fetchIdRef.current;
		setIsFetching(true);

		try {
			const result = await filesNs.list(folder);
			if (id !== fetchIdRef.current) return;
			setData(result);
			setError(null);
		} catch (err) {
			if (id !== fetchIdRef.current) return;
			setError(err instanceof Error ? err : new Error(String(err)));
		} finally {
			if (id === fetchIdRef.current) {
				setIsLoading(false);
				setIsFetching(false);
			}
		}
	}, [filesNs, folder, enabled]);

	useEffect(() => {
		void execute();
	}, [execute]);

	useEffect(() => {
		if (!refetchOnFocus || !enabled) return;
		const handler = () => {
			if (document.visibilityState === "visible") void execute();
		};
		document.addEventListener("visibilitychange", handler);
		return () => document.removeEventListener("visibilitychange", handler);
	}, [refetchOnFocus, enabled, execute]);

	useEffect(() => {
		if (!refetchInterval || !enabled || !filesNs) return;
		const id = setInterval(() => void execute(), refetchInterval);
		return () => clearInterval(id);
	}, [refetchInterval, enabled, filesNs, execute]);

	return {
		files: data.files,
		total: data.total,
		isLoading,
		isFetching,
		error,
		refetch: execute,
	};
}

// ── useUpload ─────────────────────────────────────────────────────────
// Wraps files.upload with progress-like state for UI feedback.

export type UseUploadResult = {
	upload: (
		file: File | Blob | { uri: string; type: string; name: string },
		opts?: UploadOptions,
	) => Promise<FileRecord>;
	isUploading: boolean;
	error: Error | null;
	lastUpload: FileRecord | null;
	reset: () => void;
};

export function useUpload(): UseUploadResult {
	const ctx = useContext(FFDBContext);
	const filesNs = ctx?.client?.files as FilesNamespace | undefined;

	const [isUploading, setIsUploading] = useState(false);
	const [error, setError] = useState<Error | null>(null);
	const [lastUpload, setLastUpload] = useState<FileRecord | null>(null);

	const upload = useCallback(
		async (
			file: File | Blob | { uri: string; type: string; name: string },
			opts?: UploadOptions,
		): Promise<FileRecord> => {
			if (!filesNs) {
				throw new FileStorageError(
					"FFDB client is not initialized. Make sure you are inside an <FFDBProvider>.",
					"CLIENT_NOT_READY",
				);
			}

			setIsUploading(true);
			setError(null);

			try {
				const record = await filesNs.upload(file, opts);
				setLastUpload(record);
				return record;
			} catch (err) {
				const e = err instanceof Error ? err : new Error(String(err));
				setError(e);
				throw e;
			} finally {
				setIsUploading(false);
			}
		},
		[filesNs],
	);

	const reset = useCallback(() => {
		setError(null);
		setLastUpload(null);
		setIsUploading(false);
	}, []);

	return { upload, isUploading, error, lastUpload, reset };
}

// ── useFiles ──────────────────────────────────────────────────────────
// Convenience hook that returns the full files namespace, or throws if
// the client isn't ready. Mirrors the useDB() / useAuth() pattern.

export function useFiles(): FilesNamespace {
	const ctx = useContext(FFDBContext);

	if (!ctx) {
		throw new Error("useFiles must be used within an <FFDBProvider>");
	}

	const files = ctx.client?.files as FilesNamespace | undefined;

	if (!files) {
		if (ctx.error) throw ctx.error;
		throw new FileStorageError(
			"FFDB client is not initialized yet.",
			"CLIENT_NOT_READY",
		);
	}

	return files;
}
