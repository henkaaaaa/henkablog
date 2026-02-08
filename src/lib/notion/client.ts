import fs, { createWriteStream } from 'node:fs'
import { pipeline } from 'node:stream/promises'
import axios from 'axios'
import sharp from 'sharp'
import retry from 'async-retry'
import ExifTransformer from 'exif-be-gone'
import {
  NOTION_API_SECRET,
  DATABASE_ID,
  NUMBER_OF_POSTS_PER_PAGE,
  REQUEST_TIMEOUT_MS,
} from '../../server-constants'
import type { AxiosResponse } from 'axios'
import type * as responses from './responses'
import type * as requestParams from './request-params'
import type {
  Database,
  Post,
  Block,
  FileObject,
  Emoji,
  SelectProperty,
} from '../interfaces'
import { Client, APIResponseError } from '@notionhq/client'

const client = new Client({
  auth: NOTION_API_SECRET,
})

let postsCache: Post[] | null = null
let dbCache: Database | null = null

const numberOfRetry = 2

export async function getDatabase(): Promise<Database> {
  if (dbCache !== null) return dbCache
  const res = await retry(
    async () => {
      return (await client.databases.retrieve({
        database_id: DATABASE_ID,
      })) as responses.RetrieveDatabaseResponse
    },
    { retries: numberOfRetry }
  )

  let icon: FileObject | Emoji | null = null
  if (res.icon) {
    if (res.icon.type === 'emoji') {
      icon = { Type: 'emoji', Emoji: res.icon.emoji }
    } else {
      icon = { Type: res.icon.type, Url: (res.icon as any)[res.icon.type]?.url || '' }
    }
  }

  dbCache = {
    Title: res.title.map((richText) => richText.plain_text).join(''),
    Description: res.description.map((richText) => richText.plain_text).join(''),
    Icon: icon,
    Cover: res.cover ? { Type: res.cover.type, Url: (res.cover as any)[res.cover.type]?.url || '' } : null,
  }
  return dbCache
}

export async function getAllPosts(): Promise<Post[]> {
  if (postsCache !== null) return postsCache
  let results: responses.PageObject[] = []
  const params: any = {
    database_id: DATABASE_ID,
    filter: {
     // and を削除し、Published のチェックだけに絞る
     property: 'Published', 
     checkbox: { equals: true }
  },
  sorts: [{ property: 'Date', direction: 'descending' }],
  page_size: 100,
}
  while (true) {
    const res = (await client.databases.query(params)) as responses.QueryDatabaseResponse
    results = results.concat(res.results)
    if (!res.has_more) break
    params.start_cursor = res.next_cursor
  }
  postsCache = results.map((pageObject) => _buildPost(pageObject))
  return postsCache
}

export async function getPosts(pageSize = 10): Promise<Post[]> {
  const allPosts = await getAllPosts()
  return allPosts.slice(0, pageSize)
}

export async function getPostBySlug(slug: string): Promise<Post | null> {
  const allPosts = await getAllPosts()
  return allPosts.find((post) => post.Slug === slug) || null
}

export async function getPostsByTag(tagName: string, pageSize = 10): Promise<Post[]> {
  const allPosts = await getAllPosts()
  return allPosts.filter((p) => p.Tags.some((t) => t.name === tagName)).slice(0, pageSize)
}

export async function getRankedPosts(pageSize = 10): Promise<Post[]> {
  const allPosts = await getAllPosts()
  return allPosts.filter((post) => !!post.Rank).sort((a, b) => (b.Rank || 0) - (a.Rank || 0)).slice(0, pageSize)
}

export async function getNumberOfPages(): Promise<number> {
  const allPosts = await getAllPosts()
  return Math.ceil(allPosts.length / NUMBER_OF_POSTS_PER_PAGE)
}

export async function getAllTags(): Promise<SelectProperty[]> {
  const allPosts = await getAllPosts()
  const tagNames: string[] = []
  return allPosts
    .flatMap((post) => post.Tags)
    .reduce((acc, tag) => {
      if (!tagNames.includes(tag.name)) {
        acc.push(tag); tagNames.push(tag.name)
      }
      return acc
    }, [] as SelectProperty[])
    .sort((a, b) => a.name.localeCompare(b.name))
}

export async function getAllBlocksByBlockId(blockId: string): Promise<Block[]> {
  let results: responses.BlockObject[] = []
  const params: any = { block_id: blockId }
  while (true) {
    const res = (await client.blocks.children.list(params)) as responses.RetrieveBlockChildrenResponse
    results = results.concat(res.results)
    if (!res.has_more) break
    params.start_cursor = res.next_cursor
  }
  return results.map((blockObject) => ({
    Id: blockObject.id,
    Type: blockObject.type,
    HasChildren: blockObject.has_children,
    [blockObject.type]: (blockObject as any)[blockObject.type],
  })) as Block[]
}

function _buildPost(pageObject: responses.PageObject): Post {
  const prop = pageObject.properties
  let featuredImage: any = null
  if (prop.FeaturedImage?.files && prop.FeaturedImage.files.length > 0) {
    const file = prop.FeaturedImage.files[0]
    featuredImage = {
      Type: prop.FeaturedImage.type,
      Url: file.external?.url || file.file?.url || '',
      ExpiryTime: file.file?.expiry_time,
    }
  }
  return {
    PageId: pageObject.id,
    Title: prop.Page?.title ? prop.Page.title.map((t: any) => t.plain_text).join('') : '',
    Slug: prop.Slug?.rich_text ? prop.Slug.rich_text.map((t: any) => t.plain_text).join('') : '',
    Date: prop.Date?.date ? prop.Date.date.start : '',
    LastUpdatedDate: prop.LastUpdatedDate?.date ? prop.LastUpdatedDate.date.start : '',
    Excerpt: prop.Excerpt?.rich_text ? prop.Excerpt.rich_text.map((t: any) => t.plain_text).join('') : '',
    Tags: prop.Category?.select ? [prop.Category.select] : (prop.Category?.multi_select ? prop.Category.multi_select : []),
    Status: prop.Status?.select || null,
    FeaturedImage: featuredImage,
    Rank: prop.Rank?.number || 0,
  }
}

export async function getBlock(blockId: string): Promise<any> { return {} }
export async function downloadFile(url: URL) { return Promise.resolve() }
export async function getPostsByFilter(filter: any, pageSize = 10): Promise<Post[]> {
  const allPosts = await getAllPosts(); return allPosts.slice(0, pageSize)
}