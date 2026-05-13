export type CompressOptions = {
	maxWidth?: number;
	maxHeight?: number;
	quality?: number;
};

function isBrowser(): boolean {
	return typeof document !== "undefined" && typeof HTMLCanvasElement !== "undefined";
}

export async function compressImage(
	file: File | Blob,
	opts: CompressOptions = {},
): Promise<Blob> {
	if (!isBrowser()) return file;

	const { maxWidth = 1920, maxHeight = 1080, quality = 0.8 } = opts;

	const bitmap = await createImageBitmap(file);
	let { width, height } = bitmap;

	if (width > maxWidth || height > maxHeight) {
		const ratio = Math.min(maxWidth / width, maxHeight / height);
		width = Math.round(width * ratio);
		height = Math.round(height * ratio);
	}

	const canvas = document.createElement("canvas");
	canvas.width = width;
	canvas.height = height;
	const ctx = canvas.getContext("2d")!;
	ctx.drawImage(bitmap, 0, 0, width, height);
	bitmap.close();

	const outputType = file.type === "image/png" ? "image/png" : "image/jpeg";

	return new Promise<Blob>((resolve) => {
		canvas.toBlob(
			(blob) => resolve(blob ?? file),
			outputType,
			quality,
		);
	});
}
