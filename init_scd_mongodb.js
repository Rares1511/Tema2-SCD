db.createUser({
  user: 'scd_user',
  pwd: 'scd_password',
  roles: [
    {
      role: 'readWrite',
      db: 'scd_tema2'
    }
  ]
});