// @generated automatically by Diesel CLI.

diesel::table! {
    requests (id) {
        id -> Int4,
        user_agent -> Text,
    }
}
