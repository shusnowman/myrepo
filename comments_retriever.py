import re
import vk_api
import pandas as pd
from typing import List
import conf

def get_com_context(owner_id, comment_id, vk):
    try:
        com = vk.wall.getComment(owner_id=owner_id, comment_id=comment_id)
        text = com['items'][0]['text']
        whom = com['items'][0]['from_id']
        return text, whom
    except KeyError:
        return 'not found', 'not found'

def create_session(access_token=None, login=None, password=None):
    if login is not None and access_token is not None:
        vk_session = vk_api.VkApi(login=login, token=access_token)
    elif login is not None and password is not None:
        vk_session = vk_api.VkApi(login=login, password=password)
    else:
        raise ValueError("Please provide login and access_token or password")
    vk_session.auth(token_only=True)
    vk = vk_session.get_api()
    return vk

def make_df(vk, domains: List[str], count, max_comments=100):
    df = pd.DataFrame(columns=['author',
                               'text',
                               'whom',
                               'context',
                               'level',
                               'post'])
    row = 0
    
    for domain in domains:
        for i in range(count // 100 + 1):
            wall = vk.wall.get(domain=domain,
                               count=min(100, count),
                               offset=i * 100)['items']
            for post in wall:
                first_level = vk.wall.getComments(
                    owner_id=post['owner_id'],
                    post_id=post['id'],
                    extended=1,
                    count = max_comments
                ).get('items')
                try:
                    for user in first_level:
                        level = 1
                        who = user['from_id']
                        text = user['text']
                        whom, text_reply = 'post', 'post'
                        df.loc[row] = who, text, whom, text_reply, level, post['id']
                        row += 1
            
                        if user['thread']['count'] != 0:  # продолжение треда
                            second_level = vk.wall.getComments(
                                owner_id = post['owner_id'],
                                count=100,
                                post_id=post['id'],
                                comment_id=user['id']
                            ).get('items')[:max_comments]
            
                            for user2 in second_level:
                                level = 2
                                who = user2['from_id']
                                text = user2['text']
                                if 'reply_to_comment' in user2:
                                    text_reply, whom = get_com_context(
                                        owner_id=post['owner_id'],
                                        comment_id=user2['reply_to_comment'],
                                        vk = vk
                                    )
                                else:
                                    text_reply, whom = get_com_context(
                                        owner_id=post['owner_id'],
                                        comment_id=user2['parents_stack'],
                                        vk = vk
                                    )
            
                                df.loc[row] = who, text, whom, text_reply, level, post['id']
                                row += 1
                except KeyError:
                    pass
    return df[["text", "context"]]

def scraper(link, n_posts, login = 'номер телефона', n_comments = 100, token = None, password = 'пароль'):
    vk = create_session(access_token=token, login=login, password = password)
    domain = re.search(r".+/(.+)", link).group(1)
    df = make_df(vk=vk, domains=[domain],
                 count=n_posts, max_comments=n_comments)
    return df


scraper(link="https://vk.com/milotushechki", n_posts=2)
print('yes')
