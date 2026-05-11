import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const IMAGES_BASE_DIR = path.resolve(__dirname, '../../assets/images/test_data/all');

export default class Images {
    constructor(app) {
        this.app = app;

        this.app.expressApp.get('/image/*', (req, res) => {
            const requestedPath = req.params[0];
            const resolvedPath = path.resolve(IMAGES_BASE_DIR, requestedPath);

            // Prevent path traversal outside of the images directory
            if (!resolvedPath.startsWith(IMAGES_BASE_DIR + path.sep) && resolvedPath !== IMAGES_BASE_DIR) {
                return res.status(400).send('Invalid path');
            }

            if (!fs.existsSync(resolvedPath) || !fs.statSync(resolvedPath).isFile()) {
                return res.status(404).send('Image not found');
            }

            res.sendFile(resolvedPath);
        });

        this.app.expressApp.get('/images', (req, res) => {
            const testDataDir = IMAGES_BASE_DIR;
            let entries;
            try {
                entries = fs.readdirSync(testDataDir);
            } catch (e) {
                return res.status(500).json({ error: 'Could not read directory' });
            }

            const files = entries
                .map(name => {
                    const filePath = path.join(testDataDir, name);
                    const stat = fs.statSync(filePath);
                    if (!stat.isFile()) return null;
                    const ext = path.extname(name).toLowerCase().replace('.', '') || null;
                    return {
                        filename: name,
                        size: stat.size,
                        type: ext,
                        lastModified: stat.mtime.toISOString(),
                    };
                })
                .filter(Boolean);

            res.header('Content-Type', 'application/json');
            res.end(JSON.stringify(files, null, 2));
        });
    }
}