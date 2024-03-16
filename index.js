import https from 'node:https'
import path from 'node:path'
import {createReadStream, existsSync} from 'node:fs'
import {readdir, readFile, mkdir} from 'node:fs/promises'
import tar from 'tar'
import zlib from "node:zlib";

const downloadFrom = "https://api.rutracker.cc/v1/static/pvc/f-all.tar"
const downloadTo = "data"

console.time('Total')
function downloadAndExtractTar(url, file) {
    console.time('Downloading tar')
    return new Promise((res, rej) => {
        https.get(url, {}, function (response) {
            if (response.statusCode >= 400) {
                rej(response.statusCode)
            }
            response.on('error', (err) => rej(err))
            response.on('end', () => {
                console.timeEnd('Downloading tar')
                res(true)
            });
            response.pipe(tar.x({
                cwd: file,
            }))
        });
    })

}

async function getKeeperIds() {
    return readFile('./ids.json', {encoding: 'utf-8'}).then(res => JSON.parse(res))
}

// temp solution
if (!existsSync(downloadTo)) {
    await mkdir(downloadTo)
}


const [/*downloadStatus,*/ keepersIds] = await Promise.all([
    // downloadAndExtractTar(downloadFrom, downloadTo),
    getKeeperIds()
])

const result = keepersIds.reduce((acc, id) => ({...acc, [id]: 0}), {})

const files = await readdir(downloadTo)

const parsings = files.map(file => new Promise((res) => {
    const chunks = []
    const fullPath = path.join(downloadTo, file)
    const unzipStream = zlib.createGunzip()
    createReadStream(fullPath).pipe(unzipStream)
    unzipStream.on('data', (chunk) => chunks.push(chunk))
    unzipStream.on('error', (e) => {
        throw new Error(`Error while unzip ${fullPath}`, {cause: e})
    })
    unzipStream.on('finish', () => {
        const data = JSON.parse(Buffer.concat(chunks).toString())
        for (const resultKey in (data?.result || {})) {
            for (const id of data?.result?.[resultKey]?.[5]) {
                if (keepersIds.includes(id.toString())) {
                    result[id]++
                }
            }
        }
        res()
    })
}))

await Promise.all(parsings)
console.log('Result:', result)