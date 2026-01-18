const fs = require('fs');
const path = require('path');

const packageJsonPath = path.resolve(__dirname, '../package.json');
const packageJson = require(packageJsonPath);

const now = new Date();
const year = now.getFullYear().toString().slice(-2);
const month = (now.getMonth() + 1).toString().padStart(2, '0');
const day = now.getDate().toString().padStart(2, '0');
const hour = now.getHours().toString().padStart(2, '0');
const minute = now.getMinutes().toString().padStart(2, '0');

const newVersion = `3.0.${year}${month}${day}${hour}${minute}`;

console.log(`Bumping version from ${packageJson.version} to ${newVersion}`);

packageJson.version = newVersion;

fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2) + '\n');
