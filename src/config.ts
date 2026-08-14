// Swap to tom@tompickup.co.uk once mailbox routing is live; every surface imports from here.
export const CONTACT_EMAIL = 'tom.pickup@lancashire.gov.uk';

// Mailing list. Paste the EmailOctopus embedded-form action here and the signup
// form posts straight to the list. Until then /updates falls back to composing a
// mail in the visitor's own client, which routes signups to CONTACT_EMAIL above.
// That fallback should not outlive the provider setup: CONTACT_EMAIL is a council
// address, and the list carries party-political content.
// Format: https://eocampaign1.com/forms/<form-id>
export const LIST_ENDPOINT = '';
