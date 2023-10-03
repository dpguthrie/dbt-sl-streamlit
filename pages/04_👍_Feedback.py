# third party
import requests
import streamlit as st


URL = "https://api.github.com/repos/dpguthrie/dbt-sl-streamlit/issues"


def post_issue(title: str, body: str):
    headers = {'Authorization': f'Bearer {st.secrets["GITHUB_TOKEN"]}'}
    payload = {'title': title, 'body': body}
    response = requests.post(URL, json=payload, headers=headers)
    return response


st.write('# Feedback')


with st.form('github_issue_form'):
    required_fields = {
        'feedback_title': 'Title',
        'feedback_description': 'Description'
    }
    st.write('Submit an Issue')
    title = st.text_input(
        label='Title', value='', key='feedback_title', placeholder='Required'
    )
    description = st.text_area(
        label='Describe Issue',
        value='',
        key='feedback_description',
        placeholder='Required'
    )
    email = st.text_input(
        label='Email Address',
        value='',
        key='feedback_email',
        placeholder='Optional',
    )
    submitted = st.form_submit_button('Submit')
    if submitted:
        for key, value in required_fields.items():
            if getattr(st.session_state, key) == '':
                st.error(f'{value} is a required field!')
                st.stop()
                
        if email != '':
            description += f'Email Address: {email}\n\n{description}'
        response = post_issue(title, description)
        if response.status_code == 201:
            url = response.json()['html_url']
            st.success(f'Thanks for creating an issue.  You can view the issue [here]({url})')
        else:
            st.error(response.text)
