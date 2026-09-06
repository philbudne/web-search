import * as React from 'react';
import { useState, useEffect } from 'react';
import Avatar from '@mui/material/Avatar';
import Button from '@mui/material/Button';
import CssBaseline from '@mui/material/CssBaseline';
import TextField from '@mui/material/TextField';
import Grid from '@mui/material/Grid';
import Box from '@mui/material/Box';
import LockOutlinedIcon from '@mui/icons-material/LockOutlined';
import Typography from '@mui/material/Typography';
import Alert from '@mui/material/Alert';
import Container from '@mui/material/Container';
import { FormControlLabel, Checkbox } from '@mui/material';
import { Link as MuiLink } from '@mui/material';
import { useSnackbar } from 'notistack';
import { useNavigate, Link } from 'react-router-dom';

import MatchingPasswords from './MatchingPasswords';
import { CsrfToken } from '../../services/csrfToken';
import { useRegisterMutation, useRequestResetCodeEmailMutation } from '../../app/services/authApi';

export default function SignUp() {
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();

  // register user
  const [register, { isLoading, error: isRegisterError }] = useRegisterMutation();

  const [requestResetEmail, { isLoading: isLoadingEmail, isError }] = useRequestResetCodeEmailMutation();

  // credentials
  const [formState, setFormState] = useState({
    first_name: '',
    last_name: '',
    email: '',
    password1: '',
    password2: '',
    notes: '',
  });

  const handleChange = ({ target: { name, value } }) => (
    setFormState((prev) => ({ ...prev, [name]: value.trim() }))
  );

  // MatchingPasswords element
  const [passwordData, setPasswordData] = useState({ password: '', isValid: false });
  const handlePasswordChange = (data) => {
    setPasswordData(data);
    const pw = data.password.trim();
    setFormState((prev) => ({ ...prev, password1: pw, password2: pw }));
  };

  // for terms of use checkbox
  // initialize accepted checkbox state as false
  const [accepted, setAccepted] = useState(false);

  const handleAcceptedChange = (e) => (
    setAccepted(e.target.checked)
  );

  return (
    <div>
      <Container maxWidth="md">
        <CssBaseline />
        <Box
          sx={{
            marginTop: 8,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
          }}
        >
          <Avatar sx={{ m: 1, bgcolor: 'secondary.main' }}>
            <LockOutlinedIcon titleAccess="admin only" />
          </Avatar>

          {isError && (
            <Alert severity="error" sx={{ width: '100%', mb: 2 }}>
              There was an error sending the confirmation email, please refresh and try again.
            </Alert>
          )}

          {isRegisterError && (
            <Alert severity="error" sx={{ width: '100%', mb: 2 }}>
              There was an error creating your account {isRegisterError.data.message} please refresh and try again.
            </Alert>
          )}

          <Typography component="h1" variant="h5">
            Sign up
          </Typography>

          <Typography component="h3" variant="h5">
            After Signing Up, please check your email to confirm your account.
          </Typography>

          <Box
            component="form"
            noValidate
            sx={{ mt: 3 }}
          >

            <Grid container spacing={2}>

              {/* First Name */}
              <Grid item xs={12} sm={6}>
                <TextField
                  required
                  fullWidth
                  label="First Name"
                  name="first_name"
                  autoComplete="given-name"
                  autoFocus
                  onChange={handleChange}
                />
              </Grid>

              {/* Last Name */}
              <Grid item xs={12} sm={6}>
                <TextField
                  required
                  fullWidth
                  label="Last Name"
                  name="last_name"
                  type="name"
                  autoComplete="family-name"
                  onChange={handleChange}
                />
              </Grid>

              {/* Email */}
              <Grid item xs={12}>
                <TextField
                  required
                  fullWidth
                  label="Email Address"
                  name="email"
                  type="email"
                  autoComplete="email"
                  onChange={handleChange}
                />
              </Grid>

              {/* Passwords & complaints */}
              <Grid item xs={12}>
                <MatchingPasswords
                 onChange={handlePasswordChange}
                />
              </Grid>

              {/* Notes */}
              <Grid item xs={12}>
                <TextField
                  required
                  fullWidth
                  multiline
                  name="notes"
                  rows={4}
                  label="Tell us a little about why you want to use Media Cloud"
                  type="text"
                  onChange={handleChange}
                />
              </Grid>

              {/* terms of use checkbox */}
              <Grid item xs={12}>
		<FormControlLabel
                  required
		  control={
		    <Checkbox
		      checked={accepted}
		      onChange={handleAcceptedChange}
		      name="terms"
		      color="primary"
		    />
		  }
                  label={
		    <span>
		      I accept the{' '}
		      <MuiLink
			href="https://www.mediacloud.org/legal/media-cloud-terms-of-use"
			target="_blank"
			rel="noopener noreferrer"
			onClick={(event) => {
			  // Crucial: Stops the click from checking/unchecking the checkbox
			  event.stopPropagation();
			}}
		      >
                      terms of use
		      </MuiLink>
		    </span>
                  }
		/>
              </Grid>

            </Grid>


            {/* SignUp Button */}
            <Button
              fullWidth
              variant="contained"
              sx={{ mt: 3, mb: 2 }}
              disabled={isLoading || !accepted || !passwordData.isValid}
              onClick={async () => {
                try {
                  // creating user
                  const user = await register(formState).unwrap();
                  await requestResetEmail({ email: user.email, reset_type: 'email-confirm' });
                  navigate('/');
                  enqueueSnackbar('Your account has been created, please check your email for a confirmation link', { variant: 'success' });
                } catch (err) {
                  const errorMsg = `Failed - ${err.data.message}`;
                  enqueueSnackbar(errorMsg, { variant: 'error' });
                }
              }}
            >
              Sign Up
            </Button>
          </Box>
          <Typography
            sx={{
              mr: 2,
              letterSpacing: '.02rem',
              color: 'light-blue',
              textDecoration: 'none',
            }}
            component={Link}
            to="/sign-in"
          >
            Already Have an Account? Sign-In!
          </Typography>
        </Box>
      </Container>
    </div>
  );
}
