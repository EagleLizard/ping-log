module.exports = {
  'parserOptions': {
    'ecmaVersion': 2020,
    'sourceType': 'module',
    'parser': '@typescript-eslint/parser',
    'ecmaFeatures': {

    }
  },
  'plugins': [ '@typescript-eslint' ],
  'env': {
    'es6': true,
    'node': true,
    'mocha': true,
  },
  'extends': [
    'eslint:recommended',
    'plugin:@typescript-eslint/eslint-recommended',
    'plugin:@typescript-eslint/recommended'
  ],
  'rules': {
    'indent': [
      'error',
      2,
      {
        'MemberExpression': 1,
        'SwitchCase': 1
      }
    ],
    'linebreak-style': [
      'error',
      'unix'
    ],
    'quotes': [
      'error',
      'single'
    ],
    'semi': [
      'error',
      'always'
    ],
    'space-before-blocks': [
      'error',
      'always'
    ],
    'comma-spacing': [
      'error',
      { 'before': false, 'after': true }
    ],
    'comma-style': [
      'error', 'last'
    ],
    'comma-dangle': 'off',
    'key-spacing': [
      'error', {
        'afterColon': true,
        'mode': 'strict',
      }
    ],
    'prefer-const': [
      'off',
    ],
    'no-var': 'error',
    'prefer-arrow-callback': 'error',
    'arrow-spacing': [
      'error',
      { 'before': true, 'after': true }
    ],
    'space-infix-ops': [
      'error'
    ],
    'keyword-spacing': [
      'error',
      {
        'after': true,
        'overrides': {
          'if': { 'after': false },
          'switch': { 'after': false },
          'for': { 'after': false },
          'while': { 'after': false },
          'catch': { 'after': false },
        }
      }
    ],
    'semi-spacing': [
      'error',
      {
        'after': true
      }
    ],
    'object-shorthand': [ 'error', 'properties' ],
    'eqeqeq': [ 'error', 'always' ],
    'no-trailing-spaces': [ 'error' ],
    'eol-last': [ 'error', 'always' ],
    'no-case-declarations': [ 'off' ],
    'object-curly-spacing': [ 'error', 'always' ],
    'array-bracket-spacing': [ 'error', 'always', { 'objectsInArrays': false, 'arraysInArrays': false }],
    'space-in-parens': [ 'error' ],
    'no-multiple-empty-lines': [ 'error', { 'max': 1, 'maxBOF': 1 }],
    'newline-per-chained-call': 'off',
    'brace-style': [ 2 ],
    'space-before-function-paren': [ 'error', {
      'named': 'never',
      'anonymous': 'never',
      'asyncArrow': 'always'
    }],
    '@typescript-eslint/no-unused-vars': [ 'error', {
      args: 'none'
    }],
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
  }
};
